/**
 * Request/Response tests – ported from @platformatic/job-queue/test/request-response.test.ts
 *
 * Verifies the `enqueueAndWait` flow: enqueue a job and block until it completes,
 * covering timeouts, duplicate handling, failure propagation, and subscription cleanup.
 */
import assert from "node:assert"
import { describe, it } from "node:test"
import { Cause, Deferred, Effect, Exit, Option, Stream } from "effect"
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
    maxRetries: 3,
    resultTTL: 60_000,
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

describe("Request/Response", () => {
  // ── Basic flow ─────────────────────────────────────────────────────────────

  describe("basic flow", () => {
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

    it("should handle multiple concurrent requests", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue({ concurrency: 3, visibilityTimeout: 5_000 })
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* queue.start

          const results = yield* Effect.all(
            [
              queue.enqueueAndWait("job-1", { value: 1 }, { timeout: 5_000 }),
              queue.enqueueAndWait("job-2", { value: 2 }, { timeout: 5_000 }),
              queue.enqueueAndWait("job-3", { value: 3 }, { timeout: 5_000 })
            ],
            { concurrency: "unbounded" }
          )

          assert.deepStrictEqual(results, [{ result: 2 }, { result: 4 }, { result: 6 }])
        })
      ))
  })

  // ── Timeout handling ───────────────────────────────────────────────────────

  describe("timeout handling", () => {
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
            queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 50 })
          )
          assert.ok(Exit.isFailure(exit))
          const failure = Cause.failureOption(exit.cause)
          assert.ok(failure._tag === "Some")
          assert.strictEqual((failure.value as { _tag: string })._tag, "TimeoutError")
        })
      ))

    it("should use default timeout when not specified", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value }))
          yield* queue.start

          const result = yield* queue.enqueueAndWait("job-1", { value: 42 })
          assert.deepStrictEqual(result, { result: 42 })
        })
      ))
  })

  // ── Already-completed jobs ─────────────────────────────────────────────────

  describe("already-completed jobs", () => {
    it("should return cached result immediately", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue()
          yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
          yield* queue.start

          // First call processes the job
          const result1 = yield* queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 5_000 })
          assert.deepStrictEqual(result1, { result: 42 })

          // Second call should return cached result immediately (even with a short timeout)
          const result2 = yield* queue.enqueueAndWait("job-1", { value: 999 }, { timeout: 50 })
          assert.deepStrictEqual(result2, { result: 42 })
        })
      ))
  })

  // ── Duplicate job handling ─────────────────────────────────────────────────

  describe("duplicate job handling", () => {
    it("should wait for in-progress job to complete", () =>
      runTest(
        Effect.gen(function* () {
          let callCount = 0
          const jobStarted = yield* Deferred.make<void>()
          const jobCanComplete = yield* Deferred.make<void>()

          const queue = yield* makeTestQueue({ concurrency: 1, visibilityTimeout: 5_000 })
          yield* queue.execute((job) =>
            Effect.gen(function* () {
              callCount++
              yield* Deferred.succeed(jobStarted, undefined as void)
              yield* Deferred.await(jobCanComplete)
              return { result: job.payload.value * 2 }
            })
          )
          yield* queue.start

          // Start first request
          const fiber1 = yield* Effect.fork(
            queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 5_000 })
          )

          // Wait for job to actually start processing
          yield* Deferred.await(jobStarted)

          // Start second request with same ID while first is processing
          const fiber2 = yield* Effect.fork(
            queue.enqueueAndWait("job-1", { value: 999 }, { timeout: 5_000 })
          )

          // Let the job complete
          yield* Deferred.succeed(jobCanComplete, undefined as void)

          const result1 = yield* fiber1
          const result2 = yield* fiber2

          // Both should get the same result
          assert.deepStrictEqual(result1, { result: 42 })
          assert.deepStrictEqual(result2, { result: 42 })

          // Job should only have been executed once
          assert.strictEqual(callCount, 1)
        })
      ))

    it("should handle duplicate enqueue while job is queued", () =>
      runTest(
        Effect.gen(function* () {
          let callCount = 0

          const queue = yield* makeTestQueue({ concurrency: 1, visibilityTimeout: 5_000 })
          yield* queue.execute((job) =>
            Effect.gen(function* () {
              callCount++
              return { result: job.payload.value * 2 }
            })
          )
          yield* queue.start

          // Both requests for same job ID – the second will find a duplicate
          const results = yield* Effect.all(
            [
              queue.enqueueAndWait("slow-job", { value: 10 }, { timeout: 5_000 }),
              queue.enqueueAndWait("slow-job", { value: 20 }, { timeout: 5_000 })
            ],
            { concurrency: "unbounded" }
          )

          // Both should get the same result (first payload wins)
          assert.deepStrictEqual(results[0], results[1])

          // Job should only have been executed once
          assert.strictEqual(callCount, 1)
        })
      ))
  })

  // ── Failed job handling ────────────────────────────────────────────────────

  describe("failed job handling", () => {
    it("should throw JobFailedError when job fails after max retries", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue({ concurrency: 1, visibilityTimeout: 5_000 })
          yield* queue.execute(() => Effect.fail(new Error("Job failed")))
          yield* queue.start

          const exit = yield* Effect.exit(
            queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 5_000, maxAttempts: 1 })
          )
          assert.ok(Exit.isFailure(exit))
          const failure = Cause.failureOption(exit.cause)
          assert.ok(failure._tag === "Some")
          assert.strictEqual((failure.value as { _tag: string })._tag, "JobFailedError")
        })
      ))

    it("should apply per-job resultTTL to failed job errors", () =>
      runTest(
        Effect.gen(function* () {
          const storageService = yield* StorageTag
          const queue = yield* makeTestQueue({ concurrency: 1, visibilityTimeout: 5_000 })
          yield* queue.execute(() => Effect.fail(new Error("Job failed")))
          yield* queue.start

          const exit = yield* Effect.exit(
            queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 5_000, maxAttempts: 1, resultTTL: 20 })
          )
          assert.ok(Exit.isFailure(exit))
          const failure = Cause.failureOption(exit.cause)
          assert.ok(failure._tag === "Some")
          assert.strictEqual((failure.value as { _tag: string })._tag, "JobFailedError")

          const storedError = yield* storageService.getError("job-1")
          assert.ok(storedError !== null)

          yield* Effect.sleep("60 millis")

          const expiredError = yield* storageService.getError("job-1")
          assert.strictEqual(expiredError, null)
        })
      ))

    it("should return JobFailedError for already-failed job", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue({ concurrency: 1, visibilityTimeout: 5_000 })
          yield* queue.execute(() => Effect.fail(new Error("Always fails")))
          yield* queue.start

          // First call triggers the failure
          const exit1 = yield* Effect.exit(
            queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 5_000, maxAttempts: 1 })
          )
          assert.ok(Exit.isFailure(exit1))

          // Second call should also fail immediately (cached failure)
          const exit2 = yield* Effect.exit(
            queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 100 })
          )
          assert.ok(Exit.isFailure(exit2))
          const failure = Cause.failureOption(exit2.cause)
          assert.ok(failure._tag === "Some")
          assert.strictEqual((failure.value as { _tag: string })._tag, "JobFailedError")
        })
      ))
  })

  // ── Large result handling ──────────────────────────────────────────────────

  describe("large result handling", () => {
    it("should handle large results", () =>
      runTest(
        Effect.gen(function* () {
          const queue = makeQueue<{ size: number }, { data: string }>({
            concurrency: 1,
            visibilityTimeout: 5_000
          })
          const largeQueue = yield* queue

          yield* largeQueue.execute((job) =>
            Effect.succeed({ data: "x".repeat(job.payload.size) })
          )
          yield* largeQueue.start

          const result = yield* largeQueue.enqueueAndWait("large-job", { size: 100_000 }, { timeout: 5_000 })
          assert.strictEqual(result.data.length, 100_000)
        })
      ))
  })

  // ── Subscription cleanup ───────────────────────────────────────────────────

  describe("subscription cleanup", () => {
    it("should cleanup subscriptions after timeout", () =>
      runTest(
        Effect.gen(function* () {
          // Use concurrency 2 so job-2 can run while job-1 is stuck
          const queue = yield* makeTestQueue({ concurrency: 2, visibilityTimeout: 5_000 })

          yield* queue.execute((job) =>
            Effect.gen(function* () {
              if (job.id === "job-1") {
                yield* Effect.never
              }
              return { result: (job.payload as { value: number }).value }
            })
          )
          yield* queue.start

          // This will timeout
          const exit = yield* Effect.exit(
            queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 50 })
          )
          assert.ok(Exit.isFailure(exit))
          const failure = Cause.failureOption(exit.cause)
          assert.ok(failure._tag === "Some")
          assert.strictEqual((failure.value as { _tag: string })._tag, "TimeoutError")

          // Should be able to process more jobs without subscription leaks
          const result = yield* queue.enqueueAndWait("job-2", { value: 100 }, { timeout: 5_000 })
          assert.deepStrictEqual(result, { result: 100 })
        })
      ))
  })
})
