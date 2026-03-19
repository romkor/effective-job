import { Deferred, Duration, Effect } from "effect"
import { InvalidResultTTLError, JobFailedError, TimeoutError } from "./errors.ts"
import { createJsonSerde } from "./serde.ts"
import { Storage } from "./storage/service.ts"
import type {
  CancelResult,
  EnqueueAndWaitOptions,
  EnqueueOptions,
  EnqueueResult,
  MessageStatus,
  QueueMessage,
  Serde,
  SerializedError,
  UpdateResultTTLResult
} from "./types.ts"
import { parseState } from "./utils/state.ts"

export interface ProducerConfig<TPayload, TResult> {
  readonly payloadSerde?: Serde<TPayload>
  readonly resultSerde?: Serde<TResult>
  readonly maxRetries?: number
  readonly resultTTL?: number
}

/**
 * Producer handles enqueueing jobs and retrieving results.
 * All methods return Effects requiring the Storage service.
 */
export const makeProducer = <TPayload, TResult>(config: ProducerConfig<TPayload, TResult> = {}) => {
  const payloadSerde = config.payloadSerde ?? createJsonSerde<TPayload>()
  const resultSerde = config.resultSerde ?? createJsonSerde<TResult>()
  const maxRetries = config.maxRetries ?? 3
  const defaultResultTTL = config.resultTTL ?? 3_600_000

  const validateResultTTL = (resultTTL: number): Effect.Effect<void, InvalidResultTTLError> =>
    !Number.isFinite(resultTTL) || !Number.isInteger(resultTTL) || resultTTL <= 0
      ? Effect.fail(new InvalidResultTTLError({ resultTTL }))
      : Effect.void

  const getResult = (id: string): Effect.Effect<TResult | null, never, Storage> =>
    Effect.gen(function* () {
      const storage = yield* Storage
      const buf = yield* storage.getResult(id)
      if (buf === null) return null
      return resultSerde.deserialize(buf)
    })

  const enqueue = (
    id: string,
    payload: TPayload,
    options?: EnqueueOptions
  ): Effect.Effect<EnqueueResult<TResult>, InvalidResultTTLError, Storage> =>
    Effect.gen(function* () {
      const maxAttempts = options?.maxAttempts ?? maxRetries
      const resultTTL = options?.resultTTL ?? defaultResultTTL
      yield* validateResultTTL(resultTTL)

      const timestamp = Date.now()
      const message: QueueMessage<TPayload> = {
        id,
        payload,
        createdAt: timestamp,
        attempts: 0,
        maxAttempts,
        resultTTL
      }

      const storage = yield* Storage
      const serialized = payloadSerde.serialize(message as unknown as TPayload)
      const existingState = yield* storage.enqueue(id, serialized, timestamp)

      if (existingState !== null) {
        const { status } = parseState(existingState)

        if (status === "completed") {
          const result = yield* getResult(id)
          if (result !== null) {
            return { status: "completed" as const, result }
          }
        }

        return { status: "duplicate" as const, existingState: status }
      }

      return { status: "queued" as const }
    })

  const enqueueAndWait = (
    id: string,
    payload: TPayload,
    options?: EnqueueAndWaitOptions
  ): Effect.Effect<TResult, InvalidResultTTLError | TimeoutError | JobFailedError, Storage> =>
    Effect.gen(function* () {
      const timeout = options?.timeout ?? 30_000
      const storage = yield* Storage

      // Create a Deferred that will be resolved when the job completes
      const deferred = yield* Deferred.make<TResult, JobFailedError>()

      // Subscribe BEFORE enqueue to avoid race conditions
      const unsubscribe = yield* storage.subscribeToJob(id, (status) => {
        if (status === "completed") {
          Effect.runFork(
            Effect.flatMap(getResult(id).pipe(Effect.provideService(Storage, storage)), (result) =>
              result !== null
                ? Deferred.succeed(deferred, result)
                : Effect.void
            )
          )
        } else if (status === "failed") {
          Effect.runFork(
            Effect.flatMap(storage.getError(id), (errorBuf) => {
              const msg = errorBuf ? errorBuf.toString() : "Job failed"
              return Deferred.fail(deferred, new JobFailedError({ jobId: id, originalError: msg }))
            })
          )
        }
      })

      try {
        // Enqueue the job
        const enqueueResult = yield* enqueue(id, payload, options)

        // Return cached result immediately if already completed
        if (enqueueResult.status === "completed") {
          yield* unsubscribe
          return enqueueResult.result
        }

        // If duplicate and already failed, throw immediately
        if (enqueueResult.status === "duplicate" && enqueueResult.existingState === "failed") {
          const errorBuf = yield* storage.getError(id)
          const msg = errorBuf ? errorBuf.toString() : "Job failed"
          yield* unsubscribe
          return yield* Effect.fail(new JobFailedError({ jobId: id, originalError: msg }))
        }

        // Wait for completion with timeout
        const result = yield* Deferred.await(deferred).pipe(
          Effect.timeout(Duration.millis(timeout)),
          Effect.mapError((e) => {
            if (e._tag === "TimeoutException") {
              return new TimeoutError({ jobId: id, timeout })
            }
            return e as JobFailedError
          })
        )

        return result
      } finally {
        yield* unsubscribe
      }
    })

  const cancel = (id: string): Effect.Effect<CancelResult, never, Storage> =>
    Effect.gen(function* () {
      const storage = yield* Storage
      const state = yield* storage.getJobState(id)
      if (state === null) return { status: "not_found" as const }

      const { status } = parseState(state)

      if (status === "completed") return { status: "completed" as const }
      if (status === "processing") return { status: "processing" as const }

      const deleted = yield* storage.deleteJob(id)
      return deleted ? { status: "cancelled" as const } : { status: "not_found" as const }
    })

  const updateResultTTL = (
    id: string,
    ttlMs: number
  ): Effect.Effect<UpdateResultTTLResult, InvalidResultTTLError, Storage> =>
    Effect.gen(function* () {
      yield* validateResultTTL(ttlMs)

      const storage = yield* Storage
      const state = yield* storage.getJobState(id)
      if (state === null) return { status: "not_found" as const }

      const { status } = parseState(state)
      if (status !== "completed" && status !== "failed") return { status: "not_terminal" as const }

      if (status === "completed") {
        const existing = yield* storage.getResult(id)
        if (!existing) return { status: "missing_payload" as const }
        yield* storage.setResult(id, existing, ttlMs)
        yield* storage.setJobExpiry(id, ttlMs)
        return { status: "updated" as const }
      }

      const existing = yield* storage.getError(id)
      if (!existing) return { status: "missing_payload" as const }
      yield* storage.setError(id, existing, ttlMs)
      yield* storage.setJobExpiry(id, ttlMs)
      return { status: "updated" as const }
    })

  const getStatus = (id: string): Effect.Effect<MessageStatus<TResult> | null, never, Storage> =>
    Effect.gen(function* () {
      const storage = yield* Storage
      const state = yield* storage.getJobState(id)
      if (state === null) return null

      const { status, timestamp } = parseState(state)

      const messageStatus: MessageStatus<TResult> = {
        id,
        state: status,
        createdAt: timestamp,
        attempts: 0
      }

      if (status === "completed") {
        const result = yield* getResult(id)
        if (result !== null) {
          return { ...messageStatus, result }
        }
      } else if (status === "failed") {
        const errorBuf = yield* storage.getError(id)
        if (errorBuf) {
          let errorObj: SerializedError
          try {
            errorObj = JSON.parse(errorBuf.toString()) as SerializedError
          } catch {
            errorObj = { message: errorBuf.toString() }
          }
          return { ...messageStatus, error: errorObj }
        }
      }

      return messageStatus
    })

  return {
    enqueue,
    enqueueAndWait,
    cancel,
    getResult,
    updateResultTTL,
    getStatus
  }
}
