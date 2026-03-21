/**
 * Deduplication tests – ported from @platformatic/job-queue/test/deduplication.test.ts
 *
 * Verifies that effective-job correctly deduplicates jobs across all lifecycle states:
 * queued, processing, completed, failed, and cancelled, and that re-enqueue is
 * permitted once the dedup TTL expires.
 */
import assert from "node:assert"
import { describe, it } from "node:test"
import { Deferred, Effect, Exit, Fiber, Option, Ref, Stream } from "effect"
import { makeQueue } from "../src/queue.ts"
import { MemoryStorage } from "../src/storage/memory.ts"
import { Storage as StorageTag } from "../src/storage/service.ts"
import type { QueueConfig, QueueEvent } from "../src/types.ts"

// ── Test helpers ──────────────────────────────────────────────────────────────

type TestPayload = { value: number }
type TestResult = { result: number }

const makeTestQueue = (config: QueueConfig<TestPayload, TestResult> = {}) =>
  makeQueue<TestPayload, TestResult>({
    concurrency: 1,
    visibilityTimeout: 5_000,
    ...config
  })

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const waitForEvent = (events: Stream.Stream<QueueEvent<any>>, tag: string): Effect.Effect<QueueEvent<any> | null> =>
  events.pipe(
    Stream.filter((e) => e._tag === tag),
    Stream.take(1),
    Stream.runLast,
    Effect.map(Option.getOrNull)
  )

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const runTest = (program: Effect.Effect<void, unknown, any>): Promise<void> =>
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  Effect.runPromise(Effect.scoped(program as any).pipe(Effect.provide(MemoryStorage)) as any)

// ── Tests ─────────────────────────────────────────────────────────────────────

describe("Deduplication", () => {
  // ── While queued ───────────────────────────────────────────────────────────

  describe("while queued", () => {
    it("should reject duplicate job while original is queued", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))

          // Enqueue without starting the queue (job stays in queued state)
          const result1 = yield* queue.enqueue("job-1", { value: 42 })
          assert.strictEqual(result1.status, "queued")

          // Try to enqueue same job ID
          const result2 = yield* queue.enqueue("job-1", { value: 99 })
          assert.strictEqual(result2.status, "duplicate")
          if (result2.status === "duplicate") {
            assert.strictEqual(result2.existingState, "queued")
          }
        })
      ))

    it("should return duplicate status with different payload", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))

          const result1 = yield* queue.enqueue("job-1", { value: 1 })
          assert.strictEqual(result1.status, "queued")

          // Different payload, same ID – should still be duplicate
          const result2 = yield* queue.enqueue("job-1", { value: 100 })
          assert.strictEqual(result2.status, "duplicate")
        })
      ))
  })

  // ── While processing ───────────────────────────────────────────────────────

  describe("while processing", () => {
    it("should reject duplicate job while original is processing", () =>
      runTest(
        Effect.gen(function* () {
          // A latch implemented via Deferred to signal when the handler has started
          const handlerStarted = yield* Deferred.make<void>()
          const jobCanComplete = yield* Deferred.make<void>()

          const queue = yield* makeTestQueue({ concurrency: 1, visibilityTimeout: 5_000 })
          yield* queue.execute((job) =>
            Effect.gen(function* () {
              yield* Deferred.succeed(handlerStarted, undefined as void)
              yield* Deferred.await(jobCanComplete)
              return { result: job.payload.value * 2 }
            })
          )

          yield* queue.start

          // Enqueue the job
          const result1 = yield* queue.enqueue("job-1", { value: 42 })
          assert.strictEqual(result1.status, "queued")

          // Wait for the handler to start processing
          yield* Deferred.await(handlerStarted)

          // Try to enqueue same job ID while it is processing
          const result2 = yield* queue.enqueue("job-1", { value: 99 })
          assert.strictEqual(result2.status, "duplicate")
          if (result2.status === "duplicate") {
            assert.strictEqual(result2.existingState, "processing")
          }

          // Let the job complete
          const completedFiber = yield* Effect.fork(waitForEvent(queue.events, "completed"))
          yield* Deferred.succeed(jobCanComplete, undefined as void)
          yield* completedFiber
        })
      ))
  })

  // ── After completion ───────────────────────────────────────────────────────

  describe("after completion", () => {
    it("should return cached result for completed job", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* queue.start

          // Enqueue and wait for completion
          const result1 = yield* queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 5_000 })
          assert.deepStrictEqual(result1, { result: 42 })

          // Enqueue same job ID again – should get cached result
          const result2 = yield* queue.enqueue("job-1", { value: 999 })
          assert.strictEqual(result2.status, "completed")
          if (result2.status === "completed") {
            assert.deepStrictEqual(result2.result, { result: 42 })
          }
        })
      ))

    it("should return cached result even with different payload", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* queue.start

          yield* queue.enqueueAndWait("job-1", { value: 5 }, { timeout: 5_000 })

          // Different payload, same ID – should still return cached result
          const result2 = yield* queue.enqueue("job-1", { value: 1_000 })
          assert.strictEqual(result2.status, "completed")
          if (result2.status === "completed") {
            // Original result (5 * 2 = 10), not 2000
            assert.deepStrictEqual(result2.result, { result: 10 })
          }
        })
      ))
  })

  // ── After failure ──────────────────────────────────────────────────────────

  describe("after failure", () => {
    it("should return duplicate with failed state for failed job", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue({ concurrency: 1, visibilityTimeout: 5_000 })
          yield* queue.execute(() => Effect.fail(new Error("Job failed")))
          yield* queue.start

          const failedFiber = yield* Effect.fork(waitForEvent(queue.events, "failed"))
          yield* queue.enqueue("job-1", { value: 42 })
          yield* failedFiber

          // Enqueue same job ID – should see failed status as duplicate
          const result2 = yield* queue.enqueue("job-1", { value: 99 })
          assert.strictEqual(result2.status, "duplicate")
          if (result2.status === "duplicate") {
            assert.strictEqual(result2.existingState, "failed")
          }
        })
      ))
  })

  // ── After cancellation ─────────────────────────────────────────────────────

  describe("after cancellation", () => {
    it("should allow new job after cancelled job", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))

          // Enqueue without starting the queue
          const result1 = yield* queue.enqueue("job-1", { value: 42 })
          assert.strictEqual(result1.status, "queued")

          // Cancel it
          const cancelResult = yield* queue.cancel("job-1")
          assert.strictEqual(cancelResult.status, "cancelled")

          // Now it should be possible to enqueue the same ID again
          const result2 = yield* queue.enqueue("job-1", { value: 99 })
          assert.strictEqual(result2.status, "queued")
        })
      ))
  })

  // ── Concurrent enqueue ─────────────────────────────────────────────────────

  describe("concurrent enqueue", () => {
    it("should handle concurrent enqueue of same job ID", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))

          // Start multiple enqueues concurrently
          const results = yield* Effect.all(
            [
              queue.enqueue("job-1", { value: 1 }),
              queue.enqueue("job-1", { value: 2 }),
              queue.enqueue("job-1", { value: 3 })
            ],
            { concurrency: "unbounded" }
          )

          // Exactly one should be queued, others should be duplicate
          const queued = results.filter((r) => r.status === "queued")
          const duplicates = results.filter((r) => r.status === "duplicate")

          assert.strictEqual(queued.length, 1)
          assert.strictEqual(duplicates.length, 2)
        })
      ))
  })

  // ── Result TTL ─────────────────────────────────────────────────────────────

  describe("result TTL", () => {
    it("should expire result after TTL", () =>
      runTest(
        Effect.gen(function* () {
          const storageService = yield* StorageTag

          const queue = yield* makeTestQueue({ resultTTL: 50, concurrency: 1, visibilityTimeout: 5_000 })
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* queue.start

          // Complete a job
          const result1 = yield* queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 5_000 })
          assert.deepStrictEqual(result1, { result: 42 })

          // Wait for TTL to expire
          yield* Effect.sleep("100 millis")

          // Result should be expired
          const cachedResult = yield* storageService.getResult("job-1")
          assert.strictEqual(cachedResult, null, "Result should be expired")
        })
      ))
  })

  // ── Dedup expiry ───────────────────────────────────────────────────────────

  describe("dedup expiry", () => {
    it("should allow re-enqueue after resultTTL expires for completed job", () =>
      runTest(
        Effect.gen(function* () {
          const callCountRef = yield* Ref.make(0)

          const queue = yield* makeTestQueue({ resultTTL: 50, concurrency: 1, visibilityTimeout: 5_000 })
          yield* queue.execute((job) =>
            Effect.gen(function* () {
              const count = yield* Ref.updateAndGet(callCountRef, (n) => n + 1)
              return { result: job.payload.value * count }
            })
          )
          yield* queue.start

          // Complete first job
          const result1 = yield* queue.enqueueAndWait("job-1", { value: 10 }, { timeout: 5_000 })
          assert.deepStrictEqual(result1, { result: 10 })
          assert.strictEqual(yield* Ref.get(callCountRef), 1)

          // Wait for TTL to expire
          yield* Effect.sleep("100 millis")

          // Re-enqueue same ID – should succeed now that dedup has expired
          const result2 = yield* queue.enqueueAndWait("job-1", { value: 10 }, { timeout: 5_000 })
          assert.deepStrictEqual(result2, { result: 20 })
          assert.strictEqual(yield* Ref.get(callCountRef), 2)
        })
      ))

    it("should allow re-enqueue after errorTTL expires for failed job", () =>
      runTest(
        Effect.gen(function* () {
          const shouldFailRef = yield* Ref.make(true)

          const queue = yield* makeTestQueue({ resultTTL: 50, concurrency: 1, visibilityTimeout: 5_000 })
          yield* queue.execute((job) =>
            Effect.gen(function* () {
              const shouldFail = yield* Ref.get(shouldFailRef)
              if (shouldFail) return yield* Effect.fail(new Error("Intentional failure"))
              return { result: job.payload.value * 2 }
            })
          )
          yield* queue.start

          // Enqueue and wait for failure
          const failedFiber = yield* Effect.fork(waitForEvent(queue.events, "failed"))
          yield* queue.enqueue("job-1", { value: 42 })
          yield* failedFiber

          // Verify it is a duplicate while still within TTL
          const dupResult = yield* queue.enqueue("job-1", { value: 42 })
          assert.strictEqual(dupResult.status, "duplicate")

          // Wait for TTL to expire
          yield* Effect.sleep("100 millis")

          // Now re-enqueue should work
          yield* Ref.set(shouldFailRef, false)
          const result = yield* queue.enqueueAndWait("job-1", { value: 42 }, { timeout: 5_000 })
          assert.deepStrictEqual(result, { result: 84 })
        })
      ))

    it("should still block re-enqueue within TTL window", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue({ resultTTL: 5_000, concurrency: 1, visibilityTimeout: 5_000 })
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* queue.start

          // Complete first job
          const result1 = yield* queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 5_000 })
          assert.deepStrictEqual(result1, { result: 42 })

          // Re-enqueue immediately – should return cached result (dedup still active)
          const result2 = yield* queue.enqueue("job-1", { value: 99 })
          assert.strictEqual(result2.status, "completed")
          if (result2.status === "completed") {
            assert.deepStrictEqual(result2.result, { result: 42 })
          }
        })
      ))

    it("should return null from getJobState after expiry", () =>
      runTest(
        Effect.gen(function* () {
          const storageService = yield* StorageTag
          yield* storageService.connect()

          // Simulate a completed job with a short TTL
          yield* storageService.enqueue("job-1", Buffer.from("test"), Date.now())
          yield* storageService.completeJob(
            "job-1",
            Buffer.from("test"),
            "worker-1",
            Buffer.from("result"),
            50
          )

          // State should exist within TTL
          const state1 = yield* storageService.getJobState("job-1")
          assert.ok(state1?.startsWith("completed:"))

          // Wait for TTL to expire
          yield* Effect.sleep("100 millis")

          // State should be null after expiry
          const state2 = yield* storageService.getJobState("job-1")
          assert.strictEqual(state2, null)
        })
      ))
  })
})
