/**
 * Integration tests for the PostgreSQL storage backend.
 *
 * Requires a running PostgreSQL server. The connection URL is read from the
 * `PG_TEST_URL` environment variable (defaults to the CI test container).
 *
 * Each test case runs inside its own `Effect.scoped` block.  The scope
 * finalizer automatically stops the queue and closes the DB connection.
 * Between tests the tables are truncated so each test starts clean.
 *
 * Test structure mirrors `queue.test.ts` so behaviour is verified to be
 * identical across the two storage backends.
 */
import assert from "node:assert"
import { execSync } from "node:child_process"
import { after, before, describe, it } from "node:test"
import { Cause, Effect, Exit, Layer, Option, Redacted, Stream } from "effect"
import { PgClient, layer as makePgClientLayer } from "@effect/sql-pg/PgClient"
import { SqlClient } from "@effect/sql/SqlClient"
import { makeQueue } from "../src/queue.ts"
import { makeReaper } from "../src/reaper.ts"
import { makePgStorageLayer } from "../src/storage/postgres.ts"
import { Storage as StorageTag } from "../src/storage/service.ts"
import type { QueueConfig, QueueEvent, QueueHandle } from "../src/types.ts"

// ── Connection helpers ────────────────────────────────────────────────────────

const PG_URL = process.env["PG_TEST_URL"] ?? "postgresql://ej:test@localhost:5433/effectivejob"
const TABLE_PREFIX = "ej_test"

/** PgClient Layer pointing at the test database */
const PgClientLayer = makePgClientLayer({ url: Redacted.make(PG_URL) })

/** Storage Layer wired to the test database, also exposes PgClient + SqlClient */
const PgStorage = makePgStorageLayer({ tablePrefix: TABLE_PREFIX }).pipe(
  Layer.provideMerge(PgClientLayer)
)

// ── Docker container lifecycle ────────────────────────────────────────────────

let containerId: string | null = null

before(async () => {
  // If PG_TEST_URL is explicitly provided, assume the server is already running
  if (process.env["PG_TEST_URL"]) return

  try {
    // Start a dedicated PostgreSQL container for this test run
    containerId = execSync(
      "docker run -d --rm -p 5433:5432" +
        " -e POSTGRES_PASSWORD=test -e POSTGRES_DB=effectivejob -e POSTGRES_USER=ej" +
        " postgres:16-alpine",
      { encoding: "utf8" }
    ).trim()

    // Wait for the server to accept connections (up to 15 s)
    for (let i = 0; i < 30; i++) {
      try {
        execSync(`docker exec ${containerId} pg_isready -U ej -d effectivejob -q`, { stdio: "pipe" })
        break
      } catch {
        await new Promise((r) => setTimeout(r, 500))
      }
    }
  } catch {
    // Docker unavailable – tests will fail with a clear connection error
  }
})

after(async () => {
  if (containerId) {
    try {
      execSync(`docker stop ${containerId}`, { stdio: "pipe" })
    } catch {
      // ignore
    }
    containerId = null
  }
})

// ── Test helpers ──────────────────────────────────────────────────────────────

type TestPayload = { value: number }
type TestResult = { result: number }

/** Wait for the first queue event matching `tag` */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const waitForEvent = (events: Stream.Stream<QueueEvent<any>>, tag: string): Effect.Effect<QueueEvent<any> | null> =>
  events.pipe(
    Stream.filter((e) => e._tag === tag),
    Stream.take(1),
    Stream.runLast,
    Effect.map(Option.getOrNull)
  )

const makeTestQueue = (config: QueueConfig<TestPayload, TestResult> = {}) =>
  makeQueue<TestPayload, TestResult>({
    concurrency: 1,
    maxRetries: 3,
    resultTTL: 60_000,
    visibilityTimeout: 5_000,
    ...config
  })

/** Truncate all test tables so each test starts clean */
const resetTables = Effect.gen(function* () {
  const sql = (yield* SqlClient).withoutTransforms()
  const prefix = TABLE_PREFIX
  yield* sql`TRUNCATE TABLE
    ${sql(prefix + "_queue")},
    ${sql(prefix + "_jobs")},
    ${sql(prefix + "_processing")},
    ${sql(prefix + "_results")},
    ${sql(prefix + "_errors")},
    ${sql(prefix + "_workers")},
    ${sql(prefix + "_leader_locks")}
    CASCADE`
})

/**
 * Run a test that uses `Storage` + `Scope`.
 *
 * The PgStorage layer is provided once (table creation is idempotent), then
 * all tables are truncated before running the test body.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const runTest = (program: Effect.Effect<void, unknown, any>): Promise<void> =>
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  Effect.runPromise(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    Effect.scoped(
      Effect.gen(function* () {
        yield* resetTables
        yield* program
      }) as any
    ).pipe(Effect.provide(PgStorage)) as any
  )

// ── Tests ─────────────────────────────────────────────────────────────────────

describe("Queue (PostgreSQL storage)", () => {
  // ── Lifecycle ──────────────────────────────────────────────────────────────

  describe("lifecycle", () => {
    it("should start and stop", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.start
          yield* queue.stop
        })
      ))

    it("should handle multiple start calls", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.start
          yield* queue.start
          yield* queue.stop
        })
      ))

    it("should handle multiple stop calls", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.start
          yield* queue.stop
          yield* queue.stop
        })
      ))
  })

  // ── Enqueue ────────────────────────────────────────────────────────────────

  describe("enqueue", () => {
    it("should enqueue a job", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.start
          const result = yield* queue.enqueue("job-1", { value: 42 })
          assert.strictEqual(result.status, "queued")
        })
      ))

    it("should detect duplicate jobs", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.start
          yield* queue.enqueue("job-1", { value: 42 })
          const result = yield* queue.enqueue("job-1", { value: 42 })
          assert.strictEqual(result.status, "duplicate")
        })
      ))
  })

  // ── Processing ────────────────────────────────────────────────────────────

  describe("processing", () => {
    it("should process a job and emit completed event", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* queue.start

          const completedFiber = yield* Effect.fork(waitForEvent(queue.events, "completed"))
          yield* queue.enqueue("job-1", { value: 21 })
          const event = yield* completedFiber

          assert.ok(event !== null)
          const e = event as { _tag: "completed"; id: string; result: TestResult }
          assert.strictEqual(e.id, "job-1")
          assert.deepStrictEqual(e.result, { result: 42 })
        })
      ))

    it("should store result after completion", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* queue.start

          const completedFiber = yield* Effect.fork(waitForEvent(queue.events, "completed"))
          yield* queue.enqueue("job-1", { value: 21 })
          yield* completedFiber

          const result = yield* queue.getResult("job-1")
          assert.deepStrictEqual(result, { result: 42 })
        })
      ))

    it("should return cached result for duplicate completed job", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* queue.start

          const completedFiber = yield* Effect.fork(waitForEvent(queue.events, "completed"))
          yield* queue.enqueue("job-1", { value: 21 })
          yield* completedFiber

          const duplicateResult = yield* queue.enqueue("job-1", { value: 999 })
          assert.strictEqual(duplicateResult.status, "completed")
          if (duplicateResult.status === "completed") {
            assert.deepStrictEqual(duplicateResult.result, { result: 42 })
          }
        })
      ))
  })

  // ── enqueueAndWait ────────────────────────────────────────────────────────

  describe("enqueueAndWait", () => {
    it("should wait for job result", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* queue.start
          const result = yield* queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 10_000 })
          assert.deepStrictEqual(result, { result: 42 })
        })
      ))

    it("should timeout if job takes too long", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute(() =>
            Effect.gen(function* () {
              yield* Effect.never
              return { result: 0 }
            })
          )
          yield* queue.start

          const exit = yield* Effect.exit(
            queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 100 })
          )
          assert.ok(Exit.isFailure(exit))
          const failure = Cause.failureOption(exit.cause)
          assert.ok(failure._tag === "Some")
          assert.strictEqual((failure.value as { _tag: string })._tag, "TimeoutError")
        })
      ))

    it("should return immediately for already completed job", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* queue.start

          yield* queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 10_000 })
          const result = yield* queue.enqueueAndWait("job-1", { value: 999 }, { timeout: 500 })
          assert.deepStrictEqual(result, { result: 42 })
        })
      ))
  })

  // ── Retry ─────────────────────────────────────────────────────────────────

  describe("retry", () => {
    it("should retry failed jobs", () =>
      runTest(
        Effect.gen(function* () {
          let attempts = 0
          const queue = yield* makeTestQueue()
          yield* queue.execute(() =>
            Effect.gen(function* () {
              attempts++
              if (attempts < 3) return yield* Effect.fail(new Error("temporary"))
              return { result: 100 }
            })
          )
          yield* queue.start

          const completedFiber = yield* Effect.fork(waitForEvent(queue.events, "completed"))
          yield* queue.enqueue("job-1", { value: 1 })
          yield* completedFiber

          assert.strictEqual(attempts, 3)
          const result = yield* queue.getResult("job-1")
          assert.deepStrictEqual(result, { result: 100 })
        })
      ))

    it("should emit failed event after max retries", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute(() => Effect.fail(new Error("always fails")))
          yield* queue.start

          const failedFiber = yield* Effect.fork(waitForEvent(queue.events, "failed"))
          yield* queue.enqueue("job-1", { value: 1 }, { maxAttempts: 2 })
          const event = yield* failedFiber

          assert.ok(event !== null)
          const e = event as { _tag: "failed"; id: string; error: { _tag?: string; name?: string } }
          assert.strictEqual(e.id, "job-1")
          assert.ok(e.error._tag === "MaxRetriesError" || e.error.name === "MaxRetriesError")
        })
      ))
  })

  // ── Cancel ────────────────────────────────────────────────────────────────

  describe("cancel", () => {
    it("should cancel a queued job", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.start
          yield* queue.enqueue("job-1", { value: 42 })
          const result = yield* queue.cancel("job-1")
          assert.strictEqual(result.status, "cancelled")
        })
      ))

    it("should return not_found for non-existent job", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.start
          const result = yield* queue.cancel("non-existent")
          assert.strictEqual(result.status, "not_found")
        })
      ))
  })

  // ── getStatus ─────────────────────────────────────────────────────────────

  describe("getStatus", () => {
    it("should return queued status", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.start
          yield* queue.enqueue("job-1", { value: 42 })

          const status = yield* queue.getStatus("job-1")
          assert.ok(status !== null)
          assert.strictEqual(status.id, "job-1")
          assert.strictEqual(status.state, "queued")
        })
      ))

    it("should return null for non-existent job", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.start
          const status = yield* queue.getStatus("non-existent")
          assert.strictEqual(status, null)
        })
      ))

    it("should include result for completed job", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* queue.start

          const completedFiber = yield* Effect.fork(waitForEvent(queue.events, "completed"))
          yield* queue.enqueue("job-1", { value: 21 })
          yield* completedFiber

          const status = yield* queue.getStatus("job-1")
          assert.ok(status !== null)
          assert.strictEqual(status.state, "completed")
          assert.deepStrictEqual(status.result, { result: 42 })
        })
      ))
  })

  // ── Result TTL ────────────────────────────────────────────────────────────

  describe("result TTL", () => {
    it("should override default TTL for completed jobs", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* queue.start

          const completedFiber = yield* Effect.fork(waitForEvent(queue.events, "completed"))
          yield* queue.enqueue("job-1", { value: 21 }, { resultTTL: 50 })
          yield* completedFiber

          assert.deepStrictEqual(yield* queue.getResult("job-1"), { result: 42 })
          yield* Effect.sleep("100 millis")
          assert.strictEqual(yield* queue.getResult("job-1"), null)
        })
      ))

    it("should reject invalid per-job resultTTL values", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.start
          const exit = yield* Effect.exit(queue.enqueue("job-1", { value: 1 }, { resultTTL: 0 }))
          assert.ok(Exit.isFailure(exit))
          const failure = Cause.failureOption(exit.cause)
          assert.ok(failure._tag === "Some")
          assert.strictEqual((failure.value as { _tag: string })._tag, "InvalidResultTTLError")
        })
      ))
  })

  // ── updateResultTTL ───────────────────────────────────────────────────────

  describe("updateResultTTL", () => {
    it("should update TTL for completed jobs", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue({ resultTTL: 50 })
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* queue.start
          yield* queue.enqueue("job-1", { value: 21 })
          yield* waitForEvent(queue.events, "completed")

          const updateResult = yield* queue.updateResultTTL("job-1", 500)
          assert.deepStrictEqual(updateResult, { status: "updated" })

          yield* Effect.sleep("100 millis")
          assert.deepStrictEqual(yield* queue.getResult("job-1"), { result: 42 })
        })
      ))

    it("should return not_found when job does not exist", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.start
          const result = yield* queue.updateResultTTL("missing-job", 100)
          assert.deepStrictEqual(result, { status: "not_found" })
        })
      ))

    it("should return not_terminal for queued jobs", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.start
          yield* queue.enqueue("job-1", { value: 21 })
          const result = yield* queue.updateResultTTL("job-1", 100)
          assert.deepStrictEqual(result, { status: "not_terminal" })
        })
      ))

    it("should reject invalid TTL values", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.start
          const exit = yield* Effect.exit(queue.updateResultTTL("job-1", 0))
          assert.ok(Exit.isFailure(exit))
          const failure = Cause.failureOption(exit.cause)
          assert.ok(failure._tag === "Some")
          assert.strictEqual((failure.value as { _tag: string })._tag, "InvalidResultTTLError")
        })
      ))
  })

  // ── Concurrency ───────────────────────────────────────────────────────────

  describe("concurrency", () => {
    it("should process multiple jobs concurrently", () =>
      runTest(
        Effect.gen(function* () {
          const processingTimes: number[] = []
          const startTime = Date.now()

          const localQueue = yield* makeTestQueue({ concurrency: 3, visibilityTimeout: 5_000 })
          yield* localQueue.execute((job) =>
            Effect.gen(function* () {
              processingTimes.push(Date.now() - startTime)
              yield* Effect.sleep("50 millis")
              return { result: job.payload.value }
            })
          )
          yield* localQueue.start

          yield* localQueue.enqueue("job-1", { value: 1 })
          yield* localQueue.enqueue("job-2", { value: 2 })
          yield* localQueue.enqueue("job-3", { value: 3 })

          yield* localQueue.events.pipe(
            Stream.filter((e) => e._tag === "completed"),
            Stream.take(3),
            Stream.runCollect
          )

          assert.strictEqual(processingTimes.length, 3)
          const maxDiff = Math.max(...processingTimes) - Math.min(...processingTimes)
          // PostgreSQL adds network round-trip overhead per job pickup, so we use a
          // more lenient threshold than the in-memory backend (100ms vs 200ms).
          assert.ok(maxDiff < 200, `Jobs should start concurrently, got diff of ${maxDiff}ms`)
        })
      ))
  })
})

// ── Reaper with PostgreSQL (leader election) ──────────────────────────────────

describe("Reaper (PostgreSQL storage + leader election)", () => {
  it("should start and stop", () =>
    runTest(
      Effect.gen(function* () {
        const reaper = yield* makeReaper<TestPayload>({ visibilityTimeout: 100 })
        yield* reaper.start
        yield* reaper.stop
      })
    ))

  it("should detect and recover a stalled job", () =>
    runTest(
      Effect.gen(function* () {
        // Use a queue with a long visibilityTimeout so the reaper (100ms) fires first
        const testQueue = yield* makeTestQueue({
          concurrency: 2,
          visibilityTimeout: 500,
          resultTTL: 60_000
        })
        const reaper = yield* makeReaper<TestPayload>({ visibilityTimeout: 100 })

        let processCount = 0
        let abortFirstHandler: (() => void) | undefined

        yield* testQueue.execute((job) =>
          Effect.async<TestResult, Error>((resume) => {
            processCount++
            if (processCount === 1) {
              abortFirstHandler = () => resume(Effect.fail(new Error("Aborted for cleanup")))
              job.signal.addEventListener("abort", () =>
                resume(Effect.fail(new Error("Aborted for cleanup")))
              )
            } else {
              resume(Effect.succeed({ result: job.payload.value * 2 }))
            }
          })
        )

        yield* testQueue.start
        yield* reaper.start

        const stalledFiber = yield* Effect.fork(
          reaper.events.pipe(
            Stream.filter((e) => e._tag === "stalled"),
            Stream.take(1),
            Stream.runLast
          )
        )

        const resultFiber = yield* Effect.fork(
          testQueue.enqueueAndWait("stalled-job", { value: 21 }, { timeout: 10_000 })
        )

        // Wait for stall detection
        yield* stalledFiber

        // Allow the first (stalled) handler to finish
        Effect.sync(() => abortFirstHandler?.())

        const result = yield* resultFiber
        assert.deepStrictEqual(result, { result: 42 })
        assert.strictEqual(processCount, 2, "job should have been processed twice")
      })
    ))

  it("should support leader election", () =>
    runTest(
      Effect.gen(function* () {
        const storage = yield* StorageTag

        // Acquire the lock as owner A
        const acquired = yield* storage.acquireLeaderLock!("test-lock", "owner-a", 5_000)
        assert.strictEqual(acquired, true)

        // Owner B cannot acquire it while A holds it
        const blocked = yield* storage.acquireLeaderLock!("test-lock", "owner-b", 5_000)
        assert.strictEqual(blocked, false)

        // Owner A can renew its own lock
        const renewed = yield* storage.renewLeaderLock!("test-lock", "owner-a", 5_000)
        assert.strictEqual(renewed, true)

        // Release the lock
        const released = yield* storage.releaseLeaderLock!("test-lock", "owner-a")
        assert.strictEqual(released, true)

        // Now owner B can acquire it
        const acquiredByB = yield* storage.acquireLeaderLock!("test-lock", "owner-b", 5_000)
        assert.strictEqual(acquiredByB, true)
      })
    ))
})
