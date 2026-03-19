import { Effect, Fiber, Queue as EQueue, Ref, Scope, Stream } from "effect"
import { randomUUID } from "node:crypto"
import { makeConsumer } from "./consumer.ts"
import { makeProducer } from "./producer.ts"
import { Storage } from "./storage/service.ts"
import type {
  CancelResult,
  EnqueueAndWaitOptions,
  EnqueueOptions,
  EnqueueResult,
  JobHandler,
  MessageStatus,
  QueueConfig,
  QueueEvent,
  QueueHandle,
  UpdateResultTTLResult
} from "./types.ts"

/**
 * Create a Queue handle that combines producer and consumer functionality.
 *
 * The returned handle exposes Effect-based methods for enqueueing jobs,
 * registering handlers, and observing queue events via a Stream.
 *
 * Lifecycle is managed via the explicit `start` and `stop` Effects.
 * No Storage/Scope requirements appear in the returned handle methods –
 * all dependencies are closed over at creation time.
 *
 * @example
 * ```ts
 * import { Effect } from "effect"
 * import { makeQueue } from "effective-job"
 * import { MemoryStorage } from "effective-job"
 *
 * const program = Effect.gen(function* () {
 *   const queue = yield* makeQueue<{ value: number }, { result: number }>({ concurrency: 2 })
 *
 *   yield* queue.execute((job) => Effect.succeed({ result: job.payload.value * 2 }))
 *   yield* queue.start
 *
 *   yield* queue.enqueue("job-1", { value: 21 })
 *   // ...
 *   yield* queue.stop
 * })
 *
 * Effect.runPromise(program.pipe(Effect.provide(MemoryStorage)))
 * ```
 */
export const makeQueue = <TPayload, TResult>(
  config: QueueConfig<TPayload, TResult> = {}
): Effect.Effect<QueueHandle<TPayload, TResult>, never, Storage | Scope.Scope> =>
  Effect.gen(function* () {
    const workerId = config.workerId ?? randomUUID()
    const storage = yield* Storage

    // ── Internal state ─────────────────────────────────────────────
    const startedRef = yield* Ref.make(false)
    const handlerRef = yield* Ref.make<JobHandler<TPayload, TResult> | null>(null)
    const workerFibersRef = yield* Ref.make<ReadonlyArray<Fiber.RuntimeFiber<void>>>([])
    const eventQueue = yield* EQueue.unbounded<QueueEvent<TResult>>()

    // ── Producer ───────────────────────────────────────────────────
    const producer = makeProducer<TPayload, TResult>({
      payloadSerde: config.payloadSerde,
      resultSerde: config.resultSerde,
      maxRetries: config.maxRetries,
      resultTTL: config.resultTTL
    })

    // ── Helpers ────────────────────────────────────────────────────

    const maybeStartConsumer = (): Effect.Effect<void> =>
      Effect.gen(function* () {
        const started = yield* Ref.get(startedRef)
        const handler = yield* Ref.get(handlerRef)
        const existingFibers = yield* Ref.get(workerFibersRef)

        if (started && handler !== null && existingFibers.length === 0) {
          const consumer = makeConsumer(
            {
              workerId,
              payloadSerde: config.payloadSerde,
              resultSerde: config.resultSerde,
              concurrency: config.concurrency,
              blockTimeout: config.blockTimeout,
              maxRetries: config.maxRetries,
              resultTTL: config.resultTTL,
              visibilityTimeout: config.visibilityTimeout,
              afterExecution: config.afterExecution
            },
            storage,
            handlerRef,
            eventQueue
          )
          const fibers = yield* consumer.start
          yield* Ref.set(workerFibersRef, fibers)
        }
      })

    // ── Public handle ──────────────────────────────────────────────

    const start: Effect.Effect<void> = Effect.gen(function* () {
      const alreadyStarted = yield* Ref.get(startedRef)
      if (alreadyStarted) return
      yield* storage.connect()
      yield* Ref.set(startedRef, true)
      yield* EQueue.offer(eventQueue, { _tag: "started" }).pipe(Effect.ignore)
      yield* maybeStartConsumer()
    })

    const stop: Effect.Effect<void> = Effect.gen(function* () {
      const running = yield* Ref.get(startedRef)
      if (!running) return
      yield* Ref.set(startedRef, false)

      // Interrupt all worker fibers
      const fibers = yield* Ref.get(workerFibersRef)
      yield* Effect.forEach(fibers, (f) => Fiber.interrupt(f), { concurrency: "unbounded" })
      yield* Ref.set(workerFibersRef, [])

      yield* storage.unregisterWorker(workerId)
      yield* storage.disconnect()
      yield* EQueue.offer(eventQueue, { _tag: "stopped" }).pipe(Effect.ignore)
    })

    const execute = (handler: JobHandler<TPayload, TResult>): Effect.Effect<void> =>
      Effect.gen(function* () {
        yield* Ref.set(handlerRef, handler)
        yield* maybeStartConsumer()
      })

    const enqueue = (
      id: string,
      payload: TPayload,
      options?: EnqueueOptions
    ): Effect.Effect<EnqueueResult<TResult>, unknown> =>
      producer.enqueue(id, payload, options).pipe(
        Effect.provideService(Storage, storage),
        Effect.tap((result) =>
          result.status === "queued"
            ? EQueue.offer(eventQueue, { _tag: "enqueued", id }).pipe(Effect.ignore)
            : Effect.void
        )
      )

    const enqueueAndWait = (
      id: string,
      payload: TPayload,
      options?: EnqueueAndWaitOptions
    ): Effect.Effect<TResult, unknown> =>
      producer.enqueueAndWait(id, payload, options).pipe(
        Effect.provideService(Storage, storage)
      )

    const cancel = (id: string): Effect.Effect<CancelResult> =>
      producer.cancel(id).pipe(
        Effect.provideService(Storage, storage),
        Effect.tap((result) =>
          result.status === "cancelled"
            ? EQueue.offer(eventQueue, { _tag: "cancelled", id }).pipe(Effect.ignore)
            : Effect.void
        )
      )

    const getResult = (id: string): Effect.Effect<TResult | null> =>
      producer.getResult(id).pipe(Effect.provideService(Storage, storage))

    const updateResultTTL = (
      id: string,
      ttlMs: number
    ): Effect.Effect<UpdateResultTTLResult, unknown> =>
      producer.updateResultTTL(id, ttlMs).pipe(Effect.provideService(Storage, storage))

    const getStatus = (id: string): Effect.Effect<MessageStatus<TResult> | null> =>
      producer.getStatus(id).pipe(Effect.provideService(Storage, storage))

    const events: Stream.Stream<QueueEvent<TResult>> = Stream.fromQueue(eventQueue, { shutdown: false })

    // Register cleanup finalizer so the queue is stopped when the Scope closes
    yield* Effect.addFinalizer(() => stop)

    return {
      start,
      stop,
      execute,
      enqueue,
      enqueueAndWait,
      cancel,
      getResult,
      updateResultTTL,
      getStatus,
      events
    } satisfies QueueHandle<TPayload, TResult>
  })
