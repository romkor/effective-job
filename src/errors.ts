import { Data } from "effect"

/**
 * Base class for all job queue errors
 */
export class JobQueueError extends Data.TaggedError("JobQueueError")<{
  readonly message: string
  readonly code: string
}> {}

/**
 * Thrown when enqueueAndWait times out waiting for a result
 */
export class TimeoutError extends Data.TaggedError("TimeoutError")<{
  readonly jobId: string
  readonly timeout: number
}> {
  get message(): string {
    return `Job '${this.jobId}' timed out after ${this.timeout}ms`
  }
}

/**
 * Thrown when a job exhausts all retry attempts
 */
export class MaxRetriesError extends Data.TaggedError("MaxRetriesError")<{
  readonly jobId: string
  readonly attempts: number
  readonly cause: Error
}> {
  get message(): string {
    return `Job '${this.jobId}' failed after ${this.attempts} attempts: ${this.cause.message}`
  }
}

/**
 * Thrown when a referenced job does not exist in storage
 */
export class JobNotFoundError extends Data.TaggedError("JobNotFoundError")<{
  readonly jobId: string
}> {
  get message(): string {
    return `Job '${this.jobId}' not found`
  }
}

/**
 * Thrown when a storage operation fails
 */
export class StorageError extends Data.TaggedError("StorageError")<{
  readonly message: string
  readonly cause?: Error
}> {}

/**
 * Thrown when a job was cancelled before completing
 */
export class JobCancelledError extends Data.TaggedError("JobCancelledError")<{
  readonly jobId: string
}> {
  get message(): string {
    return `Job '${this.jobId}' was cancelled`
  }
}

/**
 * Thrown by enqueueAndWait when the job fails after all retries
 */
export class JobFailedError extends Data.TaggedError("JobFailedError")<{
  readonly jobId: string
  readonly originalError: string
}> {
  get message(): string {
    return `Job '${this.jobId}' failed: ${this.originalError}`
  }
}

/**
 * Thrown when an invalid resultTTL value is provided
 */
export class InvalidResultTTLError extends Data.TaggedError("InvalidResultTTLError")<{
  readonly resultTTL: number
}> {
  get message(): string {
    return "resultTTL must be a positive integer in milliseconds"
  }
}
