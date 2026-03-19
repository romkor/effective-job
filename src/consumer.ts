import { Cause, Effect, Fiber, Queue as EQueue, Ref } from "effect"
import { MaxRetriesError } from "./errors.ts"
import { createJsonSerde } from "./serde.ts"
import { Storage } from "./storage/service.ts"
import type {
  AfterExecutionContext,
  AfterExecutionHook,
  Job,
  JobHandler,
  QueueEvent,
  QueueMessage,
  Serde
} from "./types.ts"

export interface ConsumerConfig<TPayload, TResult> {
  readonly workerId: string
  readonly payloadSerde?: Serde<TPayload>
  readonly resultSerde?: Serde<TResult>
  readonly concurrency?: number
  readonly blockTimeout?: number
  readonly maxRetries?: number
  readonly resultTTL?: number
  readonly visibilityTimeout?: number
  readonly afterExecution?: AfterExecutionHook<TPayload, TResult>
}

type ExtendedError = Error & { code?: string; toJSON?: () => Record<string, unknown> }

const noopAfterExecution = async <TPayload, TResult>(
  _context: AfterExecutionContext<TPayload, TResult>
): Promise<void> => {}

/**
 * Build a consumer that processes jobs from the queue.
 *
 * Returns a `start` Effect that, when run:
 * - Registers the worker with storage
 * - Forks `concurrency` daemon worker loops (they run until interrupted)
 * - Returns the spawned Fibers so the caller can interrupt them on `stop`
 *
 * All Effects returned close over `storage` directly (no Storage requirement in context).
 */
export const makeConsumer = <TPayload, TResult>(
  config: ConsumerConfig<TPayload, TResult>,
  storage: Storage["Type"],
  handlerRef: Ref.Ref<JobHandler<TPayload, TResult> | null>,
  eventQueue: EQueue.Queue<QueueEvent<TResult>>
) => {
  const {
    workerId,
    payloadSerde = createJsonSerde<TPayload>(),
    resultSerde = createJsonSerde<TResult>(),
    concurrency = 1,
    blockTimeout = 5,
    maxRetries = 3,
    resultTTL: defaultResultTTL = 3_600_000,
    visibilityTimeout = 30_000,
    afterExecution: afterExecutionHook = noopAfterExecution
  } = config

  const emitEvent = (event: QueueEvent<TResult>): Effect.Effect<void> =>
    EQueue.offer(eventQueue, event).pipe(Effect.ignore)

  const runAfterExecution = (
    ctx: AfterExecutionContext<TPayload, TResult>
  ): Effect.Effect<AfterExecutionContext<TPayload, TResult>> =>
    Effect.promise(async () => {
      const originalTTL = ctx.ttl
      try {
        await afterExecutionHook(ctx)
      } catch {
        ctx.ttl = originalTTL
      }
      if (!Number.isFinite(ctx.ttl) || !Number.isInteger(ctx.ttl) || ctx.ttl <= 0) {
        ctx.ttl = originalTTL
      }
      return ctx
    })

  const processJob = (message: Buffer, handler: JobHandler<TPayload, TResult>): Effect.Effect<void> =>
    Effect.gen(function* () {
      const queueMessage = payloadSerde.deserialize(message) as unknown as QueueMessage<TPayload>
      const { id, payload, attempts, maxAttempts, createdAt } = queueMessage
      const resolvedTTL = queueMessage.resultTTL ?? defaultResultTTL
      const currentAttempts = attempts + 1

      // If job was cancelled, just ack it
      const state = yield* storage.getJobState(id)
      if (state === null) {
        yield* storage.ack(id, message, workerId)
        return
      }

      const startedAt = Date.now()
      yield* storage.setJobState(id, `processing:${startedAt}:${workerId}`)
      yield* storage.publishEvent(id, "processing")

      // AbortController for backward-compatible signal (aborted on timeout)
      const abortController = new AbortController()
      const job: Job<TPayload> = { id, payload, attempts: currentAttempts, signal: abortController.signal }

      // Run handler; `Effect.exit` catches typed errors AND defects (die)
      const jobExit = yield* handler(job).pipe(
        Effect.timeout(`${visibilityTimeout} millis`),
        Effect.tapErrorCause(() => Effect.sync(() => abortController.abort())),
        Effect.exit
      )

      const finishedAt = Date.now()

      if (jobExit._tag === "Success") {
        // ── Success ────────────────────────────────────────────────
        const ctx = yield* runAfterExecution({
          id, payload, attempts: currentAttempts, maxAttempts, createdAt,
          status: "completed", result: jobExit.value, ttl: resolvedTTL,
          workerId, startedAt, finishedAt, durationMs: finishedAt - startedAt
        })
        const finalResult = ctx.result as TResult
        const serialized = resultSerde.serialize(finalResult)
        yield* storage.completeJob(id, message, workerId, serialized, ctx.ttl)
        yield* emitEvent({ _tag: "completed", id, result: finalResult })
      } else {
        // ── Failure ────────────────────────────────────────────────
        // Squash the cause to a plain Error
        const rawErr: unknown = Cause.squash(jobExit.cause)
        const err = rawErr instanceof Error ? rawErr as ExtendedError : new Error(String(rawErr)) as ExtendedError

        if (currentAttempts < maxAttempts) {
          // ── Retry ──────────────────────────────────────────────
          const updated: QueueMessage<TPayload> = { ...queueMessage, attempts: currentAttempts }
          const serializedMsg = payloadSerde.serialize(updated as unknown as TPayload)
          yield* storage.retryJob(id, serializedMsg, workerId, currentAttempts)
          yield* emitEvent({ _tag: "failing", id, error: err, attempt: currentAttempts })
        } else {
          // ── Final failure ──────────────────────────────────────
          const ctx = yield* runAfterExecution({
            id, payload, attempts: currentAttempts, maxAttempts, createdAt,
            status: "failed", error: err, ttl: resolvedTTL,
            workerId, startedAt, finishedAt, durationMs: finishedAt - startedAt
          })
          const finalError = ctx.error instanceof Error ? ctx.error as ExtendedError : err
          const maxRetriesError = new MaxRetriesError({ jobId: id, attempts: currentAttempts, cause: finalError })

          const serializedError = Buffer.from(
            JSON.stringify(
              typeof finalError.toJSON === "function"
                ? finalError.toJSON()
                : { message: finalError.message, code: finalError.code, stack: finalError.stack }
            )
          )
          yield* storage.failJob(id, message, workerId, serializedError, ctx.ttl)
          yield* emitEvent({ _tag: "failed", id, error: maxRetriesError })
        }
      }
    }).pipe(
      Effect.catchAllCause((cause) => {
        const raw = Cause.squash(cause)
        const error = raw instanceof Error ? raw : new Error(String(raw))
        return emitEvent({ _tag: "error", error })
      })
    )

  /** Continuous worker loop: dequeue → process → repeat */
  const workerLoop: Effect.Effect<void> = Effect.forever(
    Effect.gen(function* () {
      const handler = yield* Ref.get(handlerRef)
      if (handler === null) {
        yield* Effect.sleep("100 millis")
        return
      }

      const message = yield* storage.dequeue(workerId, blockTimeout)
      if (message !== null) {
        yield* processJob(message, handler)
      }
    })
  )

  /**
   * Start the consumer: register the worker and fork worker loops as daemons.
   * Returns the fibers so the caller can interrupt them on stop().
   */
  const start: Effect.Effect<ReadonlyArray<Fiber.RuntimeFiber<void>>> = Effect.gen(function* () {
    yield* storage.registerWorker(workerId, visibilityTimeout * 2)
    const fibers: Array<Fiber.RuntimeFiber<void>> = []
    for (let i = 0; i < concurrency; i++) {
      const fiber = yield* Effect.forkDaemon(workerLoop)
      fibers.push(fiber)
    }
    return fibers
  })

  return { start }
}
