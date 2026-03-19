import type { Effect } from "effect"
import type { InvalidResultTTLError } from "./errors.ts"

/**
 * Message stored in the queue
 */
export interface QueueMessage<TPayload> {
  readonly id: string
  readonly payload: TPayload
  readonly createdAt: number
  readonly attempts: number
  readonly maxAttempts: number
  readonly resultTTL?: number
  readonly correlationId?: string
}

/**
 * Job state in the jobs registry
 */
export type MessageState = "queued" | "processing" | "failing" | "completed" | "failed"

/**
 * Serialized error information
 */
export interface SerializedError {
  readonly message: string
  readonly code?: string
  readonly stack?: string
  readonly [key: string]: unknown
}

/**
 * Job status with metadata
 */
export interface MessageStatus<TResult = unknown> {
  readonly id: string
  readonly state: MessageState
  readonly createdAt: number
  readonly attempts: number
  readonly result?: TResult
  readonly error?: SerializedError
}

/**
 * Result of updating TTL for a terminal job payload
 */
export type UpdateResultTTLResult =
  | { readonly status: "updated" }
  | { readonly status: "not_found" }
  | { readonly status: "not_terminal" }
  | { readonly status: "missing_payload" }

/**
 * Options for enqueue operation
 */
export interface EnqueueOptions {
  readonly maxAttempts?: number
  readonly resultTTL?: number
}

/**
 * Options for enqueueAndWait operation
 */
export interface EnqueueAndWaitOptions extends EnqueueOptions {
  readonly timeout?: number
}

/**
 * Result of enqueue operation
 */
export type EnqueueResult<TResult = unknown> =
  | { readonly status: "queued" }
  | { readonly status: "duplicate"; readonly existingState: MessageState }
  | { readonly status: "completed"; readonly result: TResult }

/**
 * Result of cancel operation
 */
export type CancelResult =
  | { readonly status: "cancelled" }
  | { readonly status: "not_found" }
  | { readonly status: "processing" }
  | { readonly status: "completed" }

/**
 * Job passed to the handler function
 */
export interface Job<TPayload> {
  readonly id: string
  readonly payload: TPayload
  readonly attempts: number
  readonly signal: AbortSignal
}

/**
 * Context passed to the afterExecution hook
 */
export interface AfterExecutionContext<TPayload, TResult> {
  readonly id: string
  readonly payload: TPayload
  readonly attempts: number
  readonly maxAttempts: number
  readonly createdAt: number
  readonly status: "completed" | "failed"
  result?: TResult
  error?: Error
  ttl: number
  readonly workerId: string
  readonly startedAt: number
  readonly finishedAt: number
  readonly durationMs: number
}

/**
 * Hook executed after handler execution and before writing terminal state
 */
export type AfterExecutionHook<TPayload, TResult> = (
  context: AfterExecutionContext<TPayload, TResult>
) => void | Promise<void>

/**
 * Job handler function – receives a Job and returns an Effect.
 * The error channel uses `unknown` to allow any failure type.
 */
export type JobHandler<TPayload, TResult> = (
  job: Job<TPayload>
) => Effect.Effect<TResult, unknown>

/**
 * Queue configuration
 */
export interface QueueConfig<TPayload, TResult> {
  /** Unique worker ID (default: random UUID) */
  readonly workerId?: string
  /** Parallel job processing (default: 1) */
  readonly concurrency?: number
  /** Blocking dequeue timeout in seconds (default: 5) */
  readonly blockTimeout?: number
  /** Default max retry attempts (default: 3) */
  readonly maxRetries?: number
  /** Max processing time before job is considered stalled in ms (default: 30000) */
  readonly visibilityTimeout?: number
  /** TTL for stored results and errors in ms (default: 3600000 = 1 hour) */
  readonly resultTTL?: number
  /** Hook called after execution and before persisting terminal state */
  readonly afterExecution?: AfterExecutionHook<TPayload, TResult>
  /** Custom serializer for job payloads (default: JSON) */
  readonly payloadSerde?: Serde<TPayload>
  /** Custom serializer for job results (default: JSON) */
  readonly resultSerde?: Serde<TResult>
}

/**
 * Serialization / deserialization interface
 */
export interface Serde<T> {
  serialize(value: T): Buffer
  deserialize(buffer: Buffer): T
}

/**
 * Queue events emitted via the events stream
 */
export type QueueEvent<TResult> =
  | { readonly _tag: "started" }
  | { readonly _tag: "stopped" }
  | { readonly _tag: "enqueued"; readonly id: string }
  | { readonly _tag: "completed"; readonly id: string; readonly result: TResult }
  | { readonly _tag: "failed"; readonly id: string; readonly error: Error }
  | { readonly _tag: "failing"; readonly id: string; readonly error: Error; readonly attempt: number }
  | { readonly _tag: "requeued"; readonly id: string }
  | { readonly _tag: "cancelled"; readonly id: string }
  | { readonly _tag: "error"; readonly error: Error }

/**
 * The public interface of the Queue handle
 */
export interface QueueHandle<TPayload, TResult> {
  /**
   * Start the queue (connect storage and begin processing if handler registered)
   */
  readonly start: Effect.Effect<void>

  /**
   * Gracefully stop the queue (waits for in-flight jobs to complete)
   */
  readonly stop: Effect.Effect<void>

  /**
   * Register a job handler. Makes this queue a consumer.
   * Can be called before or after start().
   */
  readonly execute: (handler: JobHandler<TPayload, TResult>) => Effect.Effect<void>

  /**
   * Enqueue a job (fire-and-forget)
   */
  readonly enqueue: (
    id: string,
    payload: TPayload,
    options?: EnqueueOptions
  ) => Effect.Effect<EnqueueResult<TResult>, unknown>

  /**
   * Enqueue a job and wait for its result
   */
  readonly enqueueAndWait: (
    id: string,
    payload: TPayload,
    options?: EnqueueAndWaitOptions
  ) => Effect.Effect<TResult, unknown>

  /**
   * Cancel a pending job
   */
  readonly cancel: (id: string) => Effect.Effect<CancelResult>

  /**
   * Get the cached result of a completed job
   */
  readonly getResult: (id: string) => Effect.Effect<TResult | null>

  /**
   * Update TTL for a terminal job's stored payload
   */
  readonly updateResultTTL: (id: string, ttlMs: number) => Effect.Effect<UpdateResultTTLResult, unknown>

  /**
   * Get the current status of a job
   */
  readonly getStatus: (id: string) => Effect.Effect<MessageStatus<TResult> | null>

  /**
   * Stream of queue events (started, stopped, enqueued, completed, failed, …)
   */
  readonly events: import("effect").Stream.Stream<QueueEvent<TResult>>
}
