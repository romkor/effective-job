import { Context, Effect } from "effect"

/**
 * Shape of the Storage service – every operation returns an Effect
 */
export interface StorageShape {
  // ═══════════════════════════════════════════════════════════════════
  // LIFECYCLE
  // ═══════════════════════════════════════════════════════════════════

  /** Initialize the storage connection */
  connect(): Effect.Effect<void>

  /** Close the storage connection gracefully */
  disconnect(): Effect.Effect<void>

  // ═══════════════════════════════════════════════════════════════════
  // QUEUE OPERATIONS
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Atomically enqueue a job if not already present.
   * Returns null if job was enqueued, or the existing state string if duplicate.
   */
  enqueue(id: string, message: Buffer, timestamp: number): Effect.Effect<string | null>

  /**
   * Blocking dequeue: move a job from main queue to worker's processing queue.
   * Blocks up to `timeoutSeconds` seconds if queue is empty.
   * Returns the job message, or null on timeout.
   */
  dequeue(workerId: string, timeoutSeconds: number): Effect.Effect<Buffer | null>

  /** Move a job from a worker's processing queue back to the main queue */
  requeue(id: string, message: Buffer, workerId: string): Effect.Effect<void>

  /** Acknowledge (remove) a job from a worker's processing queue */
  ack(id: string, message: Buffer, workerId: string): Effect.Effect<void>

  // ═══════════════════════════════════════════════════════════════════
  // JOB STATE
  // ═══════════════════════════════════════════════════════════════════

  /** Get the current state of a job, or null if it doesn't exist */
  getJobState(id: string): Effect.Effect<string | null>

  /** Set the state of a job and publish a state-change notification */
  setJobState(id: string, state: string): Effect.Effect<void>

  /**
   * Delete a job from the registry (used for cancellation).
   * Returns true if the job existed and was deleted.
   */
  deleteJob(id: string): Effect.Effect<boolean>

  /** Batch fetch of job states */
  getJobStates(ids: ReadonlyArray<string>): Effect.Effect<Map<string, string | null>>

  // ═══════════════════════════════════════════════════════════════════
  // RESULTS
  // ═══════════════════════════════════════════════════════════════════

  setResult(id: string, result: Buffer, ttlMs: number): Effect.Effect<void>
  getResult(id: string): Effect.Effect<Buffer | null>
  setError(id: string, error: Buffer, ttlMs: number): Effect.Effect<void>
  getError(id: string): Effect.Effect<Buffer | null>

  // ═══════════════════════════════════════════════════════════════════
  // WORKERS
  // ═══════════════════════════════════════════════════════════════════

  registerWorker(workerId: string, ttlMs: number): Effect.Effect<void>
  refreshWorker(workerId: string, ttlMs: number): Effect.Effect<void>
  unregisterWorker(workerId: string): Effect.Effect<void>
  getWorkers(): Effect.Effect<ReadonlyArray<string>>
  getProcessingJobs(workerId: string): Effect.Effect<ReadonlyArray<Buffer>>

  // ═══════════════════════════════════════════════════════════════════
  // NOTIFICATIONS (for request/response)
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Subscribe to completion notifications for a specific job.
   * Returns an Effect that unsubscribes when run.
   */
  subscribeToJob(
    id: string,
    handler: (status: "completed" | "failed" | "failing") => void
  ): Effect.Effect<Effect.Effect<void>>

  /** Publish a job completion/failure notification */
  notifyJobComplete(id: string, status: "completed" | "failed" | "failing"): Effect.Effect<void>

  // ═══════════════════════════════════════════════════════════════════
  // EVENTS (for monitoring / reaper)
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Subscribe to all job state-change events.
   * Returns an Effect that unsubscribes when run.
   */
  subscribeToEvents(handler: (id: string, event: string) => void): Effect.Effect<Effect.Effect<void>>

  /** Publish a job state-change event */
  publishEvent(id: string, event: string): Effect.Effect<void>

  // ═══════════════════════════════════════════════════════════════════
  // LEADER ELECTION (optional, for Reaper high availability)
  // ═══════════════════════════════════════════════════════════════════

  acquireLeaderLock?(lockKey: string, ownerId: string, ttlMs: number): Effect.Effect<boolean>
  renewLeaderLock?(lockKey: string, ownerId: string, ttlMs: number): Effect.Effect<boolean>
  releaseLeaderLock?(lockKey: string, ownerId: string): Effect.Effect<boolean>

  /** Set the dedup expiry for a terminal job */
  setJobExpiry(id: string, ttlMs: number): Effect.Effect<void>

  // ═══════════════════════════════════════════════════════════════════
  // ATOMIC OPERATIONS
  // ═══════════════════════════════════════════════════════════════════

  completeJob(id: string, message: Buffer, workerId: string, result: Buffer, resultTTL: number): Effect.Effect<void>
  failJob(id: string, message: Buffer, workerId: string, error: Buffer, errorTTL: number): Effect.Effect<void>
  retryJob(id: string, message: Buffer, workerId: string, attempts: number): Effect.Effect<void>
}

/**
 * Storage service tag – use this in your Effects to require a storage backend
 */
export class Storage extends Context.Tag("@effective-job/Storage")<Storage, StorageShape>() {}
