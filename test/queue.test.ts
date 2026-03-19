/**
 * Tests for the Effect.ts port of @platformatic/job-queue.
 *
 * Each test runs inside `Effect.scoped` so lifecycle management (queue start/stop)
 * is handled automatically by the scope finalizer.
 *
 * Key differences from the original tests:
 *  - Handlers return `Effect.Effect<TResult, unknown>` instead of `Promise<TResult>`
 *  - Queue methods are run via `Effect.runPromise` inside `Effect.scoped`
 *  - Events are consumed from `queue.events` (a `Stream`) instead of an EventEmitter
 *  - Errors are `Data.TaggedError` instances (checked via `._tag`)
 */
import assert from "node:assert"
import { describe, it } from "node:test"
import { Cause, Effect, Exit, Option, Stream } from "effect"
import { makeQueue } from "../src/queue.ts"
import { MemoryStorage } from "../src/storage/memory.ts"
import { Storage as StorageTag } from "../src/storage/service.ts"
import type { QueueHandle, QueueEvent, QueueConfig } from "../src/types.ts"

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

/**
 * Build a test queue inside the current scope.
 * Requires `Storage` + `Scope` in the Effect context.
 */
const makeTestQueue = (config: QueueConfig<TestPayload, TestResult> = {}) =>
  makeQueue<TestPayload, TestResult>({
    concurrency: 1,
    maxRetries: 3,
    resultTTL: 60_000,
    visibilityTimeout: 5_000,
    ...config
  })

/**
 * Runs an Effect that uses `Storage` + `Scope` (via Effect.scoped).
 * Use inside each `it` block.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const runTest = (program: Effect.Effect<void, unknown, any>): Promise<void> =>
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  Effect.runPromise(Effect.scoped(program as any).pipe(Effect.provide(MemoryStorage)) as any)

// ── Tests ─────────────────────────────────────────────────────────────────────

describe("Queue", () => {
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

    it("should restart and keep processing jobs", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* queue.start
          yield* queue.stop
          yield* queue.start

          const completedFiber = yield* Effect.fork(waitForEvent(queue.events, "completed"))
          yield* queue.enqueue("job-after-restart", { value: 21 })
          const event = yield* completedFiber

          assert.ok(event !== null && "_tag" in event && event._tag === "completed")
          const e = event as { _tag: "completed"; id: string; result: TestResult }
          assert.strictEqual(e.id, "job-after-restart")
          assert.deepStrictEqual(e.result, { result: 42 })
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
    it("should process a job", () =>
      runTest(
        Effect.gen(function* () {
          let processed = false
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) =>
            Effect.sync(() => {
              processed = true
              return { result: job.payload.value * 2 }
            })
          )
          yield* queue.start

          const completedFiber = yield* Effect.fork(waitForEvent(queue.events, "completed"))
          yield* queue.enqueue("job-1", { value: 21 })
          yield* completedFiber

          assert.strictEqual(processed, true)
        })
      ))

    it("should emit completed event with result", () =>
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
          const result = yield* queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 5_000 })
          assert.deepStrictEqual(result, { result: 42 })
        })
      ))

    it("should timeout if job takes too long", () =>
      runTest(
        Effect.gen(function* () {
          let jobStarted = false
          const queue = yield* makeTestQueue()
          yield* queue.execute(() =>
            Effect.gen(function* () {
              jobStarted = true
              yield* Effect.never
              return { result: 0 }
            })
          )
          yield* queue.start

          const exit = yield* Effect.exit(
            queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 50 })
          )
          assert.ok(Exit.isFailure(exit))
          const failure = Cause.failureOption(exit.cause)
          assert.ok(failure._tag === "Some")
          assert.strictEqual((failure.value as { _tag: string })._tag, "TimeoutError")
          assert.strictEqual(jobStarted, true)
        })
      ))

    it("should return immediately for already completed job", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* queue.start

          yield* queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 5_000 })
          const result = yield* queue.enqueueAndWait("job-1", { value: 999 }, { timeout: 100 })
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
              if (attempts < 3) return yield* Effect.fail(new Error("Temporary failure"))
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
          yield* queue.execute(() => Effect.fail(new Error("Always fails")))
          yield* queue.start

          const failedFiber = yield* Effect.fork(waitForEvent(queue.events, "failed"))
          yield* queue.enqueue("job-1", { value: 1 }, { maxAttempts: 2 })
          const event = yield* failedFiber

          assert.ok(event !== null)
          const e = event as { _tag: "failed"; id: string; error: { _tag?: string; name?: string } }
          assert.strictEqual(e.id, "job-1")
          assert.ok(e.error)
          // MaxRetriesError has _tag from Data.TaggedError
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

    it("should not process cancelled job", () =>
      runTest(
        Effect.gen(function* () {
          const storageService = yield* StorageTag
          let processed = false

          // Manually enqueue before starting the consumer
          yield* storageService.connect()
          const msg = Buffer.from(
            JSON.stringify({
              id: "job-1",
              payload: { value: 42 },
              createdAt: Date.now(),
              attempts: 0,
              maxAttempts: 3,
              resultTTL: 60_000
            })
          )
          yield* storageService.enqueue("job-1", msg, Date.now())

          const queue = yield* makeTestQueue()
          yield* queue.cancel("job-1")

          yield* queue.execute(() =>
            Effect.sync(() => {
              processed = true
              return { result: 0 }
            })
          )
          yield* queue.start
          yield* Effect.sleep("50 millis")

          assert.strictEqual(processed, false)
        })
      ))
  })

  // ── getStatus ─────────────────────────────────────────────────────────────

  describe("getStatus", () => {
    it("should return job status", () =>
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

    it("should use error.toJSON() for failed job status when available", () =>
      runTest(
        Effect.gen(function* () {
          class JsonError extends Error {
            toJSON(): unknown {
              return {
                message: this.message,
                code: "CUSTOM_ERROR",
                details: { source: "toJSON" }
              }
            }
          }

          const queue = yield* makeTestQueue()
          yield* queue.execute(() => Effect.fail(new JsonError("Serialized by toJSON")))
          yield* queue.start

          const failedFiber = yield* Effect.fork(waitForEvent(queue.events, "failed"))
          yield* queue.enqueue("job-1", { value: 21 }, { maxAttempts: 1 })
          yield* failedFiber

          const status = yield* queue.getStatus("job-1")
          assert.ok(status !== null)
          assert.strictEqual(status.state, "failed")
          assert.deepStrictEqual(status.error, {
            message: "Serialized by toJSON",
            code: "CUSTOM_ERROR",
            details: { source: "toJSON" }
          })
        })
      ))
  })

  // ── Result TTL override ───────────────────────────────────────────────────

  describe("result TTL override", () => {
    it("should override default TTL for completed jobs", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* queue.start

          const completedFiber = yield* Effect.fork(waitForEvent(queue.events, "completed"))
          yield* queue.enqueue("job-1", { value: 21 }, { resultTTL: 20 })
          yield* completedFiber

          assert.deepStrictEqual(yield* queue.getResult("job-1"), { result: 42 })
          yield* Effect.sleep("60 millis")
          assert.strictEqual(yield* queue.getResult("job-1"), null)
        })
      ))

    it("should use the first accepted TTL when duplicates provide different values", () =>
      runTest(
        Effect.gen(function* () {
          const localQueue = yield* makeTestQueue({ resultTTL: 5_000, visibilityTimeout: 5_000 })
          yield* localQueue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))

          const first = yield* localQueue.enqueue("job-1", { value: 21 }, { resultTTL: 20 })
          const duplicate = yield* localQueue.enqueue("job-1", { value: 21 }, { resultTTL: 5_000 })

          assert.strictEqual(first.status, "queued")
          assert.strictEqual(duplicate.status, "duplicate")

          yield* localQueue.start
          yield* waitForEvent(localQueue.events, "completed")

          yield* Effect.sleep("60 millis")
          const expiredResult = yield* localQueue.getResult("job-1")
          assert.strictEqual(expiredResult, null)
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

  // ── afterExecution hook ───────────────────────────────────────────────────

  describe("afterExecution hook", () => {
    it("should allow overriding TTL and replacing result in afterExecution", () =>
      runTest(
        Effect.gen(function* () {
          const localQueue = yield* makeTestQueue({
            resultTTL: 5_000,
            visibilityTimeout: 5_000,
            afterExecution: (context) => {
              assert.strictEqual(context.status, "completed")
              assert.strictEqual(context.id, "job-1")
              assert.strictEqual((context.payload as TestPayload).value, 21)
              assert.strictEqual(context.attempts, 1)
              assert.strictEqual(context.maxAttempts, 3)
              assert.ok(context.durationMs >= 0)
              context.ttl = 20
              context.result = { result: 777 }
            }
          })

          yield* localQueue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* localQueue.start
          yield* localQueue.enqueue("job-1", { value: 21 })
          const event = yield* waitForEvent(localQueue.events, "completed")

          assert.ok(event !== null)
          const e = event as { result: TestResult }
          assert.deepStrictEqual(e.result, { result: 777 })
          assert.deepStrictEqual(yield* localQueue.getResult("job-1"), { result: 777 })

          yield* Effect.sleep("60 millis")
          assert.strictEqual(yield* localQueue.getResult("job-1"), null)
        })
      ))

    it("should support async afterExecution hook on failed jobs", () =>
      runTest(
        Effect.gen(function* () {
          const storageService = yield* StorageTag

          const localQueue = yield* makeTestQueue({
            resultTTL: 20,
            visibilityTimeout: 5_000,
            afterExecution: async (context) => {
              await new Promise((r) => setTimeout(r, 5))
              assert.strictEqual(context.status, "failed")
              context.ttl = 200
              context.error = new Error("updated boom")
            }
          })

          yield* localQueue.execute(() => Effect.fail(new Error("boom")))
          yield* localQueue.start
          yield* localQueue.enqueue("job-1", { value: 21 }, { maxAttempts: 1 })
          yield* waitForEvent(localQueue.events, "failed")

          yield* Effect.sleep("60 millis")
          const error = yield* storageService.getError("job-1")
          assert.ok(error !== null)
          assert.ok(error.toString().includes("updated boom"))
        })
      ))
  })

  // ── updateResultTTL ───────────────────────────────────────────────────────

  describe("updateResultTTL", () => {
    it("should update TTL for completed jobs", () =>
      runTest(
        Effect.gen(function* () {
          const localQueue = yield* makeTestQueue({ resultTTL: 20, visibilityTimeout: 5_000 })
          yield* localQueue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* localQueue.start
          yield* localQueue.enqueue("job-1", { value: 21 })
          yield* waitForEvent(localQueue.events, "completed")

          const updateResult = yield* localQueue.updateResultTTL("job-1", 200)
          assert.deepStrictEqual(updateResult, { status: "updated" })

          yield* Effect.sleep("60 millis")
          assert.deepStrictEqual(yield* localQueue.getResult("job-1"), { result: 42 })
        })
      ))

    it("should update TTL for failed jobs", () =>
      runTest(
        Effect.gen(function* () {
          const storageService = yield* StorageTag
          const localQueue = yield* makeTestQueue({ resultTTL: 20, visibilityTimeout: 5_000 })

          yield* localQueue.execute(() => Effect.fail(new Error("boom")))
          yield* localQueue.start
          yield* localQueue.enqueue("job-1", { value: 1 }, { maxAttempts: 1 })
          yield* waitForEvent(localQueue.events, "failed")

          const updateResult = yield* localQueue.updateResultTTL("job-1", 200)
          assert.deepStrictEqual(updateResult, { status: "updated" })

          yield* Effect.sleep("60 millis")
          const error = yield* storageService.getError("job-1")
          assert.ok(error !== null)
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

          // Wait for all 3 to complete
          yield* localQueue.events.pipe(
            Stream.filter((e) => e._tag === "completed"),
            Stream.take(3),
            Stream.runCollect
          )

          assert.strictEqual(processingTimes.length, 3)
          const maxDiff = Math.max(...processingTimes) - Math.min(...processingTimes)
          assert.ok(maxDiff < 100, `Jobs should start concurrently, got diff of ${maxDiff}ms`)
        })
      ))
  })
})
