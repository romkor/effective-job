/**
 * Tests for the Effect.ts Reaper – ported from @platformatic/job-queue.
 *
 * The Reaper monitors for stalled jobs (jobs that have been in "processing"
 * state longer than the visibility timeout) and requeues them.
 */
import assert from "node:assert"
import { describe, it } from "node:test"
import { Effect, Fiber, Stream } from "effect"
import { makeQueue } from "../src/queue.ts"
import { makeReaper } from "../src/reaper.ts"
import { MemoryStorage } from "../src/storage/memory.ts"
import { Storage as StorageTag } from "../src/storage/service.ts"
import type { QueueConfig, QueueEvent, ReaperEvent, ReaperConfig } from "../src/index.ts"

type TestPayload = { value: number }
type TestResult = { result: number }

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const runTest = (program: Effect.Effect<void, unknown, any>): Promise<void> =>
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  Effect.runPromise(Effect.scoped(program as any).pipe(Effect.provide(MemoryStorage)) as any)

/** Wait for the first reaper event matching `tag` */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const waitForReaperEvent = (events: Stream.Stream<ReaperEvent>, tag: string): Effect.Effect<ReaperEvent | null> =>
  events.pipe(
    Stream.filter((e) => e._tag === tag),
    Stream.take(1),
    Stream.runLast,
    Effect.map((opt) => (opt._tag === "Some" ? opt.value : null))
  )

/** Wait for the first queue event matching `tag` */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const waitForQueueEvent = (events: Stream.Stream<QueueEvent<any>>, tag: string): Effect.Effect<QueueEvent<any> | null> =>
  events.pipe(
    Stream.filter((e) => e._tag === tag),
    Stream.take(1),
    Stream.runLast,
    Effect.map((opt) => (opt._tag === "Some" ? opt.value : null))
  )

const makeTestQueue = (config: QueueConfig<TestPayload, TestResult> = {}) =>
  makeQueue<TestPayload, TestResult>({
    concurrency: 1,
    maxRetries: 3,
    resultTTL: 60_000,
    visibilityTimeout: 100,
    ...config
  })

const makeTestReaper = (config: ReaperConfig<TestPayload> = {}) =>
  makeReaper<TestPayload>({
    visibilityTimeout: 100,
    ...config
  })

// ── Tests ─────────────────────────────────────────────────────────────────────

describe("Reaper", () => {
  // ── Lifecycle ──────────────────────────────────────────────────────────────

  describe("lifecycle", () => {
    it("should start and stop", () =>
      runTest(
        Effect.gen(function* () {
          const reaper = yield* makeTestReaper()
          yield* reaper.start
          yield* reaper.stop
        })
      ))

    it("should handle multiple start calls", () =>
      runTest(
        Effect.gen(function* () {
          const reaper = yield* makeTestReaper()
          yield* reaper.start
          yield* reaper.start
          yield* reaper.stop
        })
      ))

    it("should handle multiple stop calls", () =>
      runTest(
        Effect.gen(function* () {
          const reaper = yield* makeTestReaper()
          yield* reaper.start
          yield* reaper.stop
          yield* reaper.stop
        })
      ))
  })

  // ── Stalled job detection ─────────────────────────────────────────────────

  describe("stalled job detection", () => {
    it("should detect and recover a stalled job", () =>
      runTest(
        Effect.gen(function* () {
          // Use concurrency 2 so the requeued job can be picked up while the first is stalled.
          // Give the queue a longer visibility timeout than the reaper.
          const testQueue = yield* makeTestQueue({ concurrency: 2, visibilityTimeout: 500, resultTTL: 60_000 })
          const reaper = yield* makeTestReaper({ visibilityTimeout: 100 })

          let processCount = 0
          let abortFirstHandler: (() => void) | undefined

          yield* testQueue.execute((job) =>
            Effect.async<TestResult, Error>((resume) => {
              processCount++
              if (processCount === 1) {
                // First attempt: stall until aborted
                abortFirstHandler = () => resume(Effect.fail(new Error("Aborted for cleanup")))
                job.signal.addEventListener("abort", () =>
                  resume(Effect.fail(new Error("Aborted for cleanup")))
                )
              } else {
                // Second attempt: complete normally
                resume(Effect.succeed({ result: job.payload.value * 2 }))
              }
            })
          )

          yield* testQueue.start
          yield* reaper.start

          const stalledFiber = yield* Effect.fork(waitForReaperEvent(reaper.events, "stalled"))
          const resultFiber = yield* Effect.fork(
            testQueue.enqueueAndWait("job-1", { value: 21 }, { timeout: 5_000 })
          )

          // Wait for stall detection
          yield* stalledFiber

          // Abort the stuck handler so it can finish
          Effect.sync(() => abortFirstHandler?.())

          const result = yield* resultFiber
          assert.deepStrictEqual(result, { result: 42 })
          assert.strictEqual(processCount, 2)
        })
      ))

    it("should emit stalled event when recovering a job", () =>
      runTest(
        Effect.gen(function* () {
          const testQueue = yield* makeTestQueue({ concurrency: 1, visibilityTimeout: 500 })
          const reaper = yield* makeTestReaper({ visibilityTimeout: 100 })

          let abortHandler: (() => void) | undefined

          yield* testQueue.execute((job) =>
            Effect.async<TestResult>((resume) => {
              abortHandler = () => resume(Effect.succeed({ result: 0 }))
              job.signal.addEventListener("abort", () => resume(Effect.succeed({ result: 0 })))
            })
          )

          yield* testQueue.start
          yield* reaper.start

          const stalledFiber = yield* Effect.fork(waitForReaperEvent(reaper.events, "stalled"))
          yield* testQueue.enqueue("job-1", { value: 21 })

          const event = yield* stalledFiber
          assert.ok(event !== null)
          assert.strictEqual((event as { _tag: string; id: string }).id, "job-1")

          // Clean up
          Effect.sync(() => abortHandler?.())
        })
      ))

    it("should not recover job that completes in time", () =>
      runTest(
        Effect.gen(function* () {
          const queue = yield* makeTestQueue({ visibilityTimeout: 100 })
          const reaper = yield* makeTestReaper({ visibilityTimeout: 100 })

          let processCount = 0
          yield* queue.execute((job) =>
            Effect.gen(function* () {
              processCount++
              return { result: job.payload.value * 2 }
            })
          )

          yield* queue.start
          yield* reaper.start

          const result = yield* queue.enqueueAndWait("job-1", { value: 21 }, { timeout: 5_000 })

          // Wait past visibility timeout
          yield* Effect.sleep("150 millis")

          assert.deepStrictEqual(result, { result: 42 })
          assert.strictEqual(processCount, 1)
        })
      ))
  })

  // ── Startup scan ──────────────────────────────────────────────────────────

  describe("periodic check", () => {
    it("should detect stalled jobs on startup", () =>
      runTest(
        Effect.gen(function* () {
          const storage = yield* StorageTag
          yield* storage.connect()

          const oldTimestamp = Date.now() - 200 // 200ms ago (past visibility timeout)

          const message = Buffer.from(
            JSON.stringify({
              id: "stalled-job",
              payload: { value: 42 },
              createdAt: oldTimestamp,
              attempts: 0,
              maxAttempts: 3,
              resultTTL: 60_000
            })
          )

          yield* storage.registerWorker("worker-1", 60_000)
          yield* storage.enqueue("stalled-job", message, oldTimestamp)
          yield* storage.dequeue("worker-1", 1)
          yield* storage.setJobState("stalled-job", `processing:${oldTimestamp}:worker-1`)

          const reaper = yield* makeTestReaper({ visibilityTimeout: 100 })

          // Fork the stalled event listener BEFORE starting the reaper
          const stalledFiber = yield* Effect.fork(waitForReaperEvent(reaper.events, "stalled"))

          yield* reaper.start

          const event = yield* stalledFiber
          assert.ok(event !== null)
          assert.strictEqual((event as { _tag: string; id: string }).id, "stalled-job")
        })
      ))

    it("should check all workers' processing queues", () =>
      runTest(
        Effect.gen(function* () {
          const storage = yield* StorageTag
          yield* storage.connect()

          const oldTimestamp = Date.now() - 200

          for (let i = 1; i <= 2; i++) {
            const workerId = `worker-${i}`
            const jobId = `stalled-job-${i}`

            yield* storage.registerWorker(workerId, 60_000)

            const message = Buffer.from(
              JSON.stringify({
                id: jobId,
                payload: { value: i },
                createdAt: oldTimestamp,
                attempts: 0,
                maxAttempts: 3,
                resultTTL: 60_000
              })
            )

            yield* storage.enqueue(jobId, message, oldTimestamp)
            yield* storage.dequeue(workerId, 1)
            yield* storage.setJobState(jobId, `processing:${oldTimestamp}:${workerId}`)
          }

          const reaper = yield* makeTestReaper({ visibilityTimeout: 100 })

          // Collect all stalled events
          const stalledFiber = yield* Effect.fork(
            reaper.events.pipe(
              Stream.filter((e) => e._tag === "stalled"),
              Stream.take(2),
              Stream.runCollect
            )
          )

          yield* reaper.start

          const events = yield* stalledFiber
          const stalledIds = [...events].map((e) => (e as { id: string }).id)

          assert.strictEqual(stalledIds.length, 2)
          assert.ok(stalledIds.includes("stalled-job-1"))
          assert.ok(stalledIds.includes("stalled-job-2"))
        })
      ))
  })

  // ── Timer management ──────────────────────────────────────────────────────

  describe("timer management", () => {
    it("should not leak timers on stop", () =>
      runTest(
        Effect.gen(function* () {
          const testQueue = yield* makeTestQueue({ concurrency: 1, visibilityTimeout: 500 })
          const reaper = yield* makeTestReaper({ visibilityTimeout: 100 })

          let abortHandler: (() => void) | undefined

          yield* testQueue.execute((job) =>
            Effect.async<TestResult>((resume) => {
              abortHandler = () => resume(Effect.succeed({ result: 0 }))
              job.signal.addEventListener("abort", () => resume(Effect.succeed({ result: 0 })))
            })
          )

          yield* testQueue.start
          yield* reaper.start

          // Start a job to trigger a timer in the reaper
          yield* testQueue.enqueue("job-1", { value: 21 })

          // Wait for the processing event to be published (timer starts)
          yield* Effect.sleep("50 millis")

          // Stop the reaper – should clear all timers
          yield* reaper.stop

          // Wait past visibility timeout – no errors should surface
          yield* Effect.sleep("150 millis")

          // Clean up the stuck handler
          Effect.sync(() => abortHandler?.())
        })
      ))
  })
})
