# Usage Guide

This guide covers everything you need to use `effective-job` effectively, from a
30-second quick start to advanced topics like custom serialization, the Reaper, and
running multiple nodes with leader election.

---

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Storage Backends](#storage-backends)
   - [In-Memory (testing / single-process)](#in-memory-testing--single-process)
   - [PostgreSQL (production)](#postgresql-production)
4. [Queue Configuration](#queue-configuration)
5. [Enqueueing Jobs](#enqueueing-jobs)
   - [Fire-and-Forget (`enqueue`)](#fire-and-forget-enqueue)
   - [Request/Response (`enqueueAndWait`)](#requestresponse-enqueueandwait)
6. [Job Handlers](#job-handlers)
7. [Retry Behaviour](#retry-behaviour)
8. [Deduplication](#deduplication)
9. [Job IDs](#job-ids)
10. [Cancellation](#cancellation)
11. [Polling Status and Fetching Results](#polling-status-and-fetching-results)
12. [Events Stream](#events-stream)
13. [afterExecution Hook](#afterexecution-hook)
14. [Updating Result TTL](#updating-result-ttl)
15. [Stall Recovery with the Reaper](#stall-recovery-with-the-reaper)
16. [Leader Election (Multi-Node Reaper)](#leader-election-multi-node-reaper)
17. [Custom Serialization](#custom-serialization)
18. [Error Handling](#error-handling)
19. [Full PostgreSQL Example](#full-postgresql-example)

---

## Installation

```sh
npm install effective-job effect
```

> **Peer dependency:** `effect` v3.20 or later is required.
> For the PostgreSQL backend, also install `@effect/sql-pg` and `@effect/sql`.

---

## Quick Start

```ts
import { Effect } from "effect"
import { makeQueue, MemoryStorage } from "effective-job"

// Define your payload and result shapes
type Payload = { value: number }
type Result  = { doubled: number }

const program = Effect.gen(function* () {
  // Create a queue (requires Storage + Scope in context)
  const queue = yield* makeQueue<Payload, Result>()

  // Register a job handler
  yield* queue.execute((job) =>
    Effect.succeed({ doubled: job.payload.value * 2 })
  )

  // Start the queue (opens storage connection + begins consuming)
  yield* queue.start

  // Enqueue a job and wait for its result
  const result = yield* queue.enqueueAndWait("job-1", { value: 21 })
  console.log(result) // { doubled: 42 }

  yield* queue.stop
})

// Wrap in a Scope and inject the storage backend
Effect.runPromise(
  Effect.scoped(program).pipe(Effect.provide(MemoryStorage))
)
```

---

## Storage Backends

### In-Memory (testing / single-process)

`MemoryStorage` (alias: `MemoryStorageLive`) lives entirely in memory. It is
ideal for tests and single-process applications that do not need durability.

```ts
import { MemoryStorage } from "effective-job"

Effect.runPromise(
  Effect.scoped(program).pipe(Effect.provide(MemoryStorage))
)
```

> **Note:** Jobs are lost if the process restarts. Workers registered in one
> process cannot share jobs with workers in another process.

### PostgreSQL (production)

The PostgreSQL backend is durable and supports multiple worker processes.
It uses `@effect/sql-pg` and automatically creates the required schema on
first connect.

```ts
import { Layer, Redacted } from "effect"
import { PgClient } from "@effect/sql-pg/PgClient"
import { makePgStorageLayer } from "effective-job"

// Build the storage layer (uses "ej" table prefix by default)
const StorageLayer = makePgStorageLayer({ tablePrefix: "ej" }).pipe(
  Layer.provide(
    PgClient.layer({ url: Redacted.make(process.env.DATABASE_URL!) })
  )
)

Effect.runPromise(
  Effect.scoped(program).pipe(Effect.provide(StorageLayer))
)
```

**Tables created** (using the default `"ej"` prefix):

| Table | Purpose |
|---|---|
| `ej_jobs` | Job state registry |
| `ej_queue` | Pending job messages |
| `ej_processing` | Per-worker in-flight messages |
| `ej_results` | Completed job results |
| `ej_errors` | Failed job error payloads |
| `ej_workers` | Active worker heartbeats |
| `ej_leader_locks` | Reaper leader election locks |

You can use a different prefix to run multiple isolated queues in the same
database schema:

```ts
const StorageLayer = makePgStorageLayer({ tablePrefix: "billing" }).pipe(...)
```

A pre-built `PgStorageLive` layer using the default prefix is also exported
for convenience:

```ts
import { PgStorageLive } from "effective-job"

const StorageLayer = PgStorageLive.pipe(
  Layer.provide(PgClient.layer({ url: Redacted.make(process.env.DATABASE_URL!) }))
)
```

---

## Queue Configuration

All options are optional. Pass them to `makeQueue`:

```ts
const queue = yield* makeQueue<Payload, Result>({
  // Unique identifier for this worker (default: random UUID)
  workerId: "worker-1",

  // How many jobs to process in parallel (default: 1)
  concurrency: 4,

  // How long (in seconds) to block waiting for a job when the queue is
  // empty (default: 5). Lower values mean faster shutdown but more polling.
  blockTimeout: 5,

  // Default maximum number of attempts before a job is marked as failed
  // (default: 3). Can be overridden per enqueue call.
  maxRetries: 3,

  // Max time (ms) a job may take before it is considered stalled and
  // eligible for reaping (default: 30000). Also used as the job handler
  // timeout — the handler's AbortSignal is aborted when this expires.
  visibilityTimeout: 30_000,

  // How long (ms) to keep completed/failed job state and results in
  // storage (default: 3600000 = 1 hour). Can be overridden per enqueue
  // call.
  resultTTL: 3_600_000,

  // Optional hook called after the handler returns, before the terminal
  // state is written to storage. See "afterExecution Hook" below.
  afterExecution: undefined,

  // Custom serializers for payloads and results. See "Custom Serialization".
  payloadSerde: undefined,
  resultSerde: undefined,
})
```

---

## Enqueueing Jobs

### Fire-and-Forget (`enqueue`)

Adds a job to the queue and returns immediately. The return value tells you
what happened:

```ts
const result = yield* queue.enqueue("job-1", { value: 42 })

switch (result.status) {
  case "queued":
    // Job was successfully enqueued
    break

  case "duplicate":
    // A job with the same ID already exists
    console.log("existing state:", result.existingState)
    // existingState: "queued" | "processing" | "failing" | "completed" | "failed"
    break

  case "completed":
    // Job already finished – result is returned directly from cache
    console.log("cached result:", result.result)
    break
}
```

Per-enqueue options override the queue defaults:

```ts
yield* queue.enqueue("job-1", payload, {
  maxAttempts: 1,      // number of attempts (overrides queue.maxRetries)
  resultTTL: 60_000,   // TTL for the stored result, in ms
})
```

### Request/Response (`enqueueAndWait`)

Enqueues a job (or joins an already-running one) and waits until it completes,
returning its result directly.

```ts
// Blocks until the job completes or the timeout fires
const result = yield* queue.enqueueAndWait("job-1", { value: 21 }, {
  timeout: 30_000,   // default: 30000 ms
  maxAttempts: 3,
  resultTTL: 3_600_000,
})
```

**Key properties:**

- If the same job ID is already in the queue (queued, processing, or failing),
  the second caller joins the wait — the handler runs **only once** and both
  callers receive the same result.
- If the job is already completed (cached), the result is returned immediately
  without waiting.
- On timeout a `TimeoutError` is thrown.
- If the job fails after all retries a `JobFailedError` is thrown.

```ts
import { Effect, Exit, Cause } from "effect"
import { TimeoutError, JobFailedError } from "effective-job"

const exit = yield* Effect.exit(
  queue.enqueueAndWait("job-1", payload, { timeout: 5_000 })
)

if (Exit.isFailure(exit)) {
  const err = Cause.failureOption(exit.cause)
  if (err._tag === "Some") {
    if (err.value._tag === "TimeoutError") { /* ... */ }
    if (err.value._tag === "JobFailedError") { /* ... */ }
  }
}
```

---

## Job Handlers

A handler is a function `(job: Job<TPayload>) => Effect.Effect<TResult, unknown>`.

```ts
yield* queue.execute((job) => {
  const { id, payload, attempts, signal } = job

  // `signal` is an AbortSignal that fires when visibilityTimeout expires.
  // Use it to cancel in-flight I/O when the job is timed out.
  signal.addEventListener("abort", () => {
    console.log(`job ${id} was aborted after attempt ${attempts}`)
  })

  return Effect.succeed({ doubled: payload.value * 2 })
})
```

- The handler can return any `Effect`, including ones that do async work via
  `Effect.promise`, `Effect.tryPromise`, or `Effect.async`.
- Any error (typed `Effect.fail` or uncaught defect via `Effect.die`) is caught
  and triggers the retry logic.
- The handler may be registered before or after `queue.start`. The consumer loop
  checks the current handler on each iteration.

**Async handler example:**

```ts
yield* queue.execute((job) =>
  Effect.tryPromise({
    try: async () => {
      const response = await fetch(`https://api.example.com/process/${job.payload.id}`, {
        signal: job.signal,  // respect abort on visibility timeout
      })
      return response.json() as Result
    },
    catch: (e) => e  // error channel is `unknown`
  })
)
```

---

## Retry Behaviour

When a handler fails (for any reason), the job is automatically retried up to
`maxRetries` (or the per-enqueue `maxAttempts`) times.

```ts
yield* queue.execute((job) =>
  Effect.gen(function* () {
    if (job.attempts < 3) {
      // job.attempts starts at 1 on the first attempt
      return yield* Effect.fail(new Error("Not ready yet"))
    }
    return { result: "ok" }
  })
)
```

Each retry increments `job.attempts`. After the final attempt the job state
becomes `"failed"`, the error is persisted, and a `"failed"` event is emitted.

**Interim events:** each failed attempt (before the last) emits a `"failing"`
event with the current `attempt` number.

---

## Deduplication

`effective-job` uses job IDs as deduplication keys. Enqueueing a job with an
ID that already exists (in any state) returns a `"duplicate"` result instead
of adding it to the queue a second time.

```
queued    → duplicate (existingState: "queued")
processing → duplicate (existingState: "processing")
failing   → duplicate (existingState: "failing")
completed → completed  (result returned from cache)
failed    → duplicate (existingState: "failed")
```

Once the `resultTTL` expires the job ID can be re-used:

```ts
// First run: processed normally
const r1 = yield* queue.enqueueAndWait("report:2024-01", payload, { resultTTL: 1_000 })

// After 1 second the record expires and the ID becomes available again
yield* Effect.sleep("2 seconds")

// Second run: treated as a fresh job
const r2 = yield* queue.enqueueAndWait("report:2024-01", payload, { resultTTL: 1_000 })
```

---

## Job IDs

A job ID can be any string. Two helpers are exported:

```ts
import { generateId, contentId } from "effective-job"

// Random UUID – use when every enqueue should be unique
const id1 = generateId()  // "f47ac10b-58cc-4372-a567-0e02b2c3d479"

// Content-based SHA-256 hash (first 16 hex chars) – use for natural deduplication
// based on the job's semantic identity
const id2 = contentId({ reportType: "monthly", month: "2024-01" })
// "a3f5e8b2c1d7e409"
```

Content IDs are useful when you want to ensure that identical work (same input
parameters) is only queued once, regardless of how many times the caller
invokes `enqueue`.

---

## Cancellation

A job in `"queued"` state can be cancelled before a worker picks it up:

```ts
const result = yield* queue.cancel("job-1")

switch (result.status) {
  case "cancelled":
    // Successfully removed from the queue
    break
  case "not_found":
    // No job with that ID exists
    break
  case "processing":
    // Job is currently being processed – cannot cancel
    break
  case "completed":
    // Job already finished – cannot cancel
    break
}
```

A cancelled job emits a `"cancelled"` event and its ID immediately becomes
available for re-use (there is no TTL on cancellations).

---

## Polling Status and Fetching Results

### `getStatus`

Returns the current status of any job, regardless of state:

```ts
const status = yield* queue.getStatus("job-1")

if (status === null) {
  // Job does not exist (or TTL has expired)
} else {
  console.log(status.state)      // "queued" | "processing" | "failing" | "completed" | "failed"
  console.log(status.createdAt)  // timestamp (ms)
  console.log(status.attempts)   // number of attempts so far

  if (status.state === "completed") {
    console.log(status.result)   // TResult
  }
  if (status.state === "failed") {
    console.log(status.error)    // SerializedError
  }
}
```

### `getResult`

Returns the stored result for a completed job, or `null` if the result has
expired or the job does not exist:

```ts
const result = yield* queue.getResult("job-1")  // TResult | null
```

---

## Events Stream

`queue.events` is a `Stream.Stream<QueueEvent<TResult>>` you can consume to
react to anything happening in the queue:

```ts
import { Stream, Effect } from "effect"

// Process all events until the stream is interrupted
yield* Effect.fork(
  queue.events.pipe(
    Stream.tap((event) =>
      Effect.sync(() => {
        switch (event._tag) {
          case "started":
            console.log("queue started")
            break
          case "stopped":
            console.log("queue stopped")
            break
          case "enqueued":
            console.log(`enqueued ${event.id}`)
            break
          case "completed":
            console.log(`completed ${event.id}`, event.result)
            break
          case "failed":
            console.log(`failed ${event.id}`, event.error)
            break
          case "failing":
            console.log(`attempt ${event.attempt} failed for ${event.id}`)
            break
          case "requeued":
            console.log(`requeued ${event.id}`)
            break
          case "cancelled":
            console.log(`cancelled ${event.id}`)
            break
          case "error":
            console.error("consumer error", event.error)
            break
        }
      })
    ),
    Stream.runDrain
  )
)
```

**Waiting for a specific event:**

```ts
import { Stream, Option } from "effect"

const completedEvent = yield* queue.events.pipe(
  Stream.filter((e) => e._tag === "completed" && e.id === "job-1"),
  Stream.take(1),
  Stream.runLast,
  Effect.map(Option.getOrNull)
)
```

---

## afterExecution Hook

The `afterExecution` hook is called after the handler returns (success or
failure) but **before** the terminal state is written to storage. This gives
you a last chance to:

- Override the result TTL
- Replace or augment the result
- Modify the stored error

The hook receives a mutable `context` object:

```ts
const queue = yield* makeQueue<Payload, Result>({
  afterExecution: async (context) => {
    console.log(`job ${context.id} ${context.status} in ${context.durationMs}ms`)

    if (context.status === "completed") {
      // Extend TTL for large jobs that took a long time
      if (context.durationMs > 5_000) {
        context.ttl = 24 * 60 * 60 * 1_000  // keep for 24 hours
      }

      // Replace the stored result (e.g. add audit metadata)
      context.result = {
        ...(context.result as Result),
        processedAt: new Date().toISOString(),
        workerId: context.workerId,
      }
    }

    if (context.status === "failed") {
      // Change the stored error message
      context.error = new Error(`Wrapped: ${context.error?.message}`)
      // Extend TTL for failed jobs so they can be inspected later
      context.ttl = 7 * 24 * 60 * 60 * 1_000
    }
  }
})
```

**Context properties:**

| Property | Type | Description |
|---|---|---|
| `id` | `string` | Job ID |
| `payload` | `TPayload` | Original job payload |
| `attempts` | `number` | Current attempt number |
| `maxAttempts` | `number` | Maximum attempts allowed |
| `createdAt` | `number` | Timestamp when job was enqueued (ms) |
| `status` | `"completed" \| "failed"` | Final execution status |
| `result` | `TResult \| undefined` | Handler's return value (**mutable**) |
| `error` | `Error \| undefined` | Thrown error (**mutable**) |
| `ttl` | `number` | Result/error TTL in ms (**mutable**) |
| `workerId` | `string` | ID of the worker that processed the job |
| `startedAt` | `number` | Timestamp when processing started (ms) |
| `finishedAt` | `number` | Timestamp when processing finished (ms) |
| `durationMs` | `number` | `finishedAt - startedAt` |

The hook may be synchronous or `async`. Any exception thrown inside the hook is
silently swallowed and the original values are restored.

---

## Updating Result TTL

After a job has completed or failed you can extend (or shorten) how long its
result stays in storage without re-running the job:

```ts
const update = yield* queue.updateResultTTL("job-1", 24 * 60 * 60 * 1_000)

switch (update.status) {
  case "updated":
    // TTL successfully changed
    break
  case "not_found":
    // Job does not exist (or already expired)
    break
  case "not_terminal":
    // Job is still queued / processing – can only update terminal jobs
    break
  case "missing_payload":
    // Terminal job found but result/error payload is missing (already expired)
    break
}
```

---

## Stall Recovery with the Reaper

The Reaper monitors for stalled jobs — jobs that remain in `"processing"` state
beyond the visibility timeout — and requeues them so they can be retried.

```ts
import { makeQueue, makeReaper, MemoryStorage } from "effective-job"
import { Effect } from "effect"

const program = Effect.gen(function* () {
  const queue = yield* makeQueue<Payload, Result>({
    concurrency: 2,
    visibilityTimeout: 30_000,  // jobs stall after 30 s
  })
  const reaper = yield* makeReaper({
    visibilityTimeout: 30_000,  // must match the queue's timeout
  })

  yield* queue.execute(myHandler)
  yield* queue.start
  yield* reaper.start

  // Monitor reaper events
  yield* Effect.fork(
    reaper.events.pipe(
      Stream.tap((e) =>
        e._tag === "stalled"
          ? Effect.log(`stalled job ${e.id} requeued`)
          : Effect.void
      ),
      Stream.runDrain
    )
  )

  // ... your application logic ...

  yield* reaper.stop
  yield* queue.stop
})

Effect.runPromise(Effect.scoped(program).pipe(Effect.provide(MemoryStorage)))
```

**Reaper configuration:**

```ts
const reaper = yield* makeReaper({
  // Maximum time (ms) a job may stay in "processing" before it is stalled.
  // Should match the queue's visibilityTimeout (default: 30000).
  visibilityTimeout: 30_000,

  // Custom payload deserializer (must match the queue's payloadSerde)
  payloadSerde: undefined,

  // Leader election – see next section
  leaderElection: { enabled: false },
})
```

**Reaper events:**

| Event `_tag` | Payload | Description |
|---|---|---|
| `"stalled"` | `{ id: string }` | Job was stalled and requeued |
| `"leadershipAcquired"` | — | This reaper instance became the leader |
| `"leadershipLost"` | — | Lock was not renewed; leadership lost |
| `"error"` | `{ error: Error }` | An unexpected error occurred |

---

## Leader Election (Multi-Node Reaper)

When running multiple instances of your application (e.g. with Kubernetes
replicas or multiple EC2 instances), you typically want **only one** Reaper to
be active at a time to avoid duplicate recovery operations.

Enable leader election to have the Reaper instances coordinate via the storage
backend:

```ts
const reaper = yield* makeReaper({
  visibilityTimeout: 30_000,
  leaderElection: {
    enabled: true,

    // How long the lock is valid before it must be renewed (ms, default: 30000)
    lockTTL: 30_000,

    // How often the leader renews its lock (ms, default: 10000)
    renewalInterval: 10_000,

    // How often non-leaders try to acquire the lock (ms, default: 5000)
    acquireRetryInterval: 5_000,
  },
})
```

> **Prerequisite:** Leader election requires a storage backend that implements
> `acquireLeaderLock`, `renewLeaderLock`, and `releaseLeaderLock`.
> **`MemoryStorage` does not support this** — use the PostgreSQL backend.
> Attempting to enable leader election with `MemoryStorage` will emit an
> `"error"` event.

**Behaviour:**

- On start, every Reaper instance races to acquire the distributed lock.
- The winner transitions to _leader_ and begins monitoring jobs.
- Followers retry every `acquireRetryInterval` ms.
- The leader renews the lock every `renewalInterval` ms.
- If the leader fails to renew (e.g. process crash), the lock expires after
  `lockTTL` ms and one of the followers takes over.

---

## Custom Serialization

By default, payloads and results are serialized to/from JSON. You can provide
custom serializers to use a more compact format (e.g. MessagePack, Protocol
Buffers, CBOR).

A serializer must implement the `Serde<T>` interface:

```ts
import type { Serde } from "effective-job"

// Example: MessagePack serializer using the `msgpackr` package
import { pack, unpack } from "msgpackr"

const msgpackSerde = <T>(): Serde<T> => ({
  serialize: (value: T): Buffer => Buffer.from(pack(value)),
  deserialize: (buffer: Buffer): T => unpack(buffer) as T,
})

const queue = yield* makeQueue<Payload, Result>({
  payloadSerde: msgpackSerde<Payload>(),
  resultSerde: msgpackSerde<Result>(),
})
```

> **Important:** The Reaper also deserializes job payloads. If you provide a
> custom `payloadSerde` to the queue, pass the same serde to the Reaper:
>
> ```ts
> const reaper = yield* makeReaper({ payloadSerde: msgpackSerde<Payload>() })
> ```

The built-in JSON serde can also be instantiated directly:

```ts
import { JsonSerde, createJsonSerde } from "effective-job"

const serde1 = new JsonSerde<Payload>()
const serde2 = createJsonSerde<Payload>()  // factory helper
```

---

## Error Handling

All errors are `Data.TaggedError` instances with a `_tag` discriminant.
Use `Effect.catchTag` / `Effect.catchTags` for structured matching:

```ts
import { Effect } from "effect"
import { TimeoutError, JobFailedError, InvalidResultTTLError } from "effective-job"

yield* queue.enqueueAndWait("job-1", payload, { timeout: 5_000 }).pipe(
  Effect.catchTag("TimeoutError", (e) => {
    console.error(`Job ${e.jobId} timed out after ${e.timeout}ms`)
    return Effect.void
  }),
  Effect.catchTag("JobFailedError", (e) => {
    console.error(`Job ${e.jobId} failed: ${e.originalError}`)
    return Effect.void
  })
)
```

**Error reference:**

| Class | `_tag` | When thrown |
|---|---|---|
| `TimeoutError` | `"TimeoutError"` | `enqueueAndWait` when the job does not finish within `timeout` ms |
| `JobFailedError` | `"JobFailedError"` | `enqueueAndWait` when the awaited job fails after all retries |
| `InvalidResultTTLError` | `"InvalidResultTTLError"` | `enqueue`, `enqueueAndWait`, or `updateResultTTL` when `resultTTL ≤ 0` |
| `MaxRetriesError` | `"MaxRetriesError"` | Emitted on the `"failed"` event when a job exhausts all attempts |
| `JobNotFoundError` | `"JobNotFoundError"` | Reserved; not thrown by the current public API |
| `JobCancelledError` | `"JobCancelledError"` | Reserved; not thrown by the current public API |
| `StorageError` | `"StorageError"` | Reserved; storage failures are currently surfaced as defects |
| `JobQueueError` | `"JobQueueError"` | Base class; not thrown directly |

**Storage defects:** if the database connection drops, the affected worker fiber
dies with a defect. This is intentional — the Reaper will detect the stalled
job and requeue it.

---

## Full PostgreSQL Example

A complete example showing a producer process, a worker process, and a Reaper
process sharing the same PostgreSQL-backed storage.

### Shared layer composition

```ts
// storage.ts
import { Layer, Redacted } from "effect"
import { PgClient } from "@effect/sql-pg/PgClient"
import { makePgStorageLayer } from "effective-job"

export const StorageLayer = makePgStorageLayer().pipe(
  Layer.provide(
    PgClient.layer({
      url: Redacted.make(process.env.DATABASE_URL!),
      // optional connection pool settings:
      // maxConnections: 10,
      // idleTimeoutMillis: 30_000,
    })
  )
)
```

### Producer

```ts
// producer.ts
import { Effect } from "effect"
import { makeQueue, generateId } from "effective-job"
import { StorageLayer } from "./storage.ts"

type EmailJob = { to: string; subject: string; body: string }
type EmailResult = { messageId: string }

const producer = Effect.gen(function* () {
  // No handler registered – this process only enqueues
  const queue = yield* makeQueue<EmailJob, EmailResult>()
  yield* queue.start

  const id = generateId()
  const result = yield* queue.enqueue(id, {
    to: "user@example.com",
    subject: "Welcome!",
    body: "Thanks for signing up.",
  })

  console.log("enqueue result:", result.status)

  yield* queue.stop
})

Effect.runPromise(
  Effect.scoped(producer).pipe(Effect.provide(StorageLayer))
)
```

### Worker

```ts
// worker.ts
import { Effect, Stream } from "effect"
import { makeQueue } from "effective-job"
import { StorageLayer } from "./storage.ts"

type EmailJob = { to: string; subject: string; body: string }
type EmailResult = { messageId: string }

const worker = Effect.gen(function* () {
  const queue = yield* makeQueue<EmailJob, EmailResult>({
    concurrency: 4,
    visibilityTimeout: 60_000,  // email sending can take up to 60 s
    resultTTL: 7 * 24 * 60 * 60 * 1_000,  // keep results for 7 days
    afterExecution: async (ctx) => {
      // Record duration in your metrics system
      console.log(`[${ctx.status}] ${ctx.id} took ${ctx.durationMs}ms`)
    },
  })

  yield* queue.execute((job) =>
    Effect.tryPromise({
      try: async () => {
        // call your SMTP / SES API here
        const messageId = await sendEmail(job.payload)
        return { messageId }
      },
      catch: (e) => e,
    })
  )

  yield* queue.start
  console.log("Worker started. Waiting for jobs...")

  // Block forever (until the process is killed)
  yield* Effect.never
})

Effect.runPromise(
  Effect.scoped(worker).pipe(Effect.provide(StorageLayer))
)

async function sendEmail(payload: EmailJob): Promise<string> {
  // stub – replace with your real implementation
  return `msg-${Date.now()}`
}
```

### Reaper

```ts
// reaper.ts
import { Effect, Stream } from "effect"
import { makeReaper } from "effective-job"
import { StorageLayer } from "./storage.ts"

const reaperProcess = Effect.gen(function* () {
  const reaper = yield* makeReaper({
    visibilityTimeout: 60_000,
    leaderElection: {
      enabled: true,
      lockTTL: 30_000,
      renewalInterval: 10_000,
      acquireRetryInterval: 5_000,
    },
  })

  yield* Effect.fork(
    reaper.events.pipe(
      Stream.tap((e) =>
        Effect.sync(() => {
          if (e._tag === "stalled")             console.log(`stalled: ${e.id}`)
          if (e._tag === "leadershipAcquired")  console.log("became leader")
          if (e._tag === "leadershipLost")      console.log("lost leadership")
          if (e._tag === "error")               console.error("reaper error:", e.error)
        })
      ),
      Stream.runDrain
    )
  )

  yield* reaper.start
  console.log("Reaper started")

  yield* Effect.never
})

Effect.runPromise(
  Effect.scoped(reaperProcess).pipe(Effect.provide(StorageLayer))
)
```

### Deployment tips

- Run **one or more** worker processes — they all share the same queue and
  distribute work automatically via `SELECT … FOR UPDATE SKIP LOCKED`.
- Run **one or more** Reaper instances with `leaderElection.enabled = true` —
  only the leader is active at any time.
- Combine the worker and the Reaper in the same process if you prefer a
  simpler deployment:

  ```ts
  const combined = Effect.gen(function* () {
    const queue  = yield* makeQueue<EmailJob, EmailResult>({ concurrency: 4 })
    const reaper = yield* makeReaper({ visibilityTimeout: 60_000 })

    yield* queue.execute(emailHandler)
    yield* queue.start
    yield* reaper.start

    yield* Effect.never
  })
  ```
