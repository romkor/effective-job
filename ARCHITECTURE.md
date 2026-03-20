# Architecture

`effective-job` is a durable job queue library written entirely in
[Effect](https://effect.website/). It is a functional port of
[@platformatic/job-queue](https://github.com/platformatic/job-queue) and shares
the same semantics (deduplication, request/response, retry, stall recovery) while
embracing Effect's type-safe composition model.

---

## Table of Contents

1. [Design Goals](#design-goals)
2. [Module Map](#module-map)
3. [Job Lifecycle](#job-lifecycle)
4. [Data Flow](#data-flow)
   - [Enqueue (fire-and-forget)](#enqueue-fire-and-forget)
   - [Enqueue-and-Wait (request/response)](#enqueue-and-wait-requestresponse)
   - [Consumer Loop](#consumer-loop)
   - [Reaper (stall recovery)](#reaper-stall-recovery)
5. [Storage Abstraction](#storage-abstraction)
   - [StorageShape Interface](#storageshape-interface)
   - [In-Memory Backend](#in-memory-backend)
   - [PostgreSQL Backend](#postgresql-backend)
6. [Effect Patterns](#effect-patterns)
7. [Error Taxonomy](#error-taxonomy)
8. [Dependency Graph](#dependency-graph)

---

## Design Goals

| Goal | Mechanism |
|---|---|
| **Type safety end-to-end** | Generic `TPayload` / `TResult` flow from `makeQueue` through storage and back to the caller |
| **Pluggable storage** | `StorageShape` interface + `Context.Tag` — swap backends by changing `Layer` composition |
| **No implicit global state** | All dependencies are provided via Effect's `Context` / `Layer` system |
| **Structured concurrency** | Worker fibers are forked, tracked in a `Ref`, and interrupted cleanly on `stop` |
| **Deduplication** | Per-job state in storage prevents the same ID from being processed twice |
| **Request/response** | `enqueueAndWait` subscribes to per-job notifications via `Deferred` before enqueueing |
| **Stall recovery** | `Reaper` sets per-job timers and requeues jobs whose processing has exceeded the visibility timeout |

---

## Module Map

```
src/
├── index.ts              — public re-exports (the entire API surface)
├── types.ts              — all shared interfaces and type aliases
├── errors.ts             — tagged errors (Data.TaggedError subclasses)
├── serde.ts              — Serde<T> interface + JsonSerde default implementation
├── queue.ts              — makeQueue() — top-level façade combining producer + consumer
├── producer.ts           — enqueueing, result lookup, cancel, updateResultTTL
├── consumer.ts           — dequeue loop, job execution, retry logic, afterExecution hook
├── reaper.ts             — stall detection, timer management, leader election
├── storage/
│   ├── service.ts        — StorageShape interface + Storage Context.Tag
│   ├── memory.ts         — MemoryStorageLive layer (in-process, for testing)
│   └── postgres.ts       — PgStorageLive / makePgStorageLayer (production)
└── utils/
    ├── id.ts             — generateId(), contentId()
    └── state.ts          — parseState() — parses "status:timestamp[:extra]" strings
```

---

## Job Lifecycle

Jobs transition through the following states, stored as `"status:timestamp[:extra]"` strings in the jobs registry:

```
                 ┌──────────────────────────────────────────┐
   enqueue()     │                                          │ TTL expires
  ─────────────► │  queued : ts                             │ ─────────────► (deleted)
                 │                                          │
                 └──────────────┬───────────────────────────┘
                                │ dequeue()
                                ▼
                 ┌──────────────────────────────────────────┐
                 │  processing : ts : workerId              │
                 └──┬──────────────────────┬────────────────┘
                    │ success              │ error (attempt < maxAttempts)
                    ▼                      ▼
  ┌────────────────────────┐  ┌────────────────────────────────┐
  │  completed : ts        │  │  failing : ts : attempt        │
  │  (result stored)       │  │  (requeued → processing again) │
  └───────────┬────────────┘  └────────────────────────────────┘
              │ TTL expires          │ error (attempt == maxAttempts)
              ▼                      ▼
          (deleted)     ┌────────────────────────┐
                        │  failed : ts           │
                        │  (error stored)        │
                        └───────────┬────────────┘
                                    │ TTL expires
                                    ▼
                                (deleted)

  cancel() while queued:
  queued ─────────────► (deleted, state removed)
```

State strings are encoded as `"status:timestamp"` or `"status:timestamp:extra"`:

| State | Format | Extra field |
|---|---|---|
| `queued` | `queued:1234567890` | — |
| `processing` | `processing:1234567890:worker-uuid` | workerId |
| `failing` | `failing:1234567890:2` | attempt count |
| `completed` | `completed:1234567890` | — |
| `failed` | `failed:1234567890` | — |

---

## Data Flow

### Enqueue (fire-and-forget)

```
caller
  │
  │ queue.enqueue(id, payload, opts?)
  ▼
producer.enqueue()
  │  validate resultTTL
  │  serialize QueueMessage → Buffer (payloadSerde)
  │
  ▼
storage.enqueue(id, message, timestamp)
  │  atomic check: does job already exist?
  │  ├─ exists + not expired → return existing state string  → "duplicate" | "completed"
  │  └─ new / expired        → write jobs[id] = "queued:ts"
  │                             push message onto main queue
  │                             publish "queued" event
  │                             return null
  ▼
EnqueueResult
  { status: "queued" }
  { status: "duplicate", existingState }
  { status: "completed", result }       ← result fetched from storage
```

### Enqueue-and-Wait (request/response)

```
caller
  │
  │ queue.enqueueAndWait(id, payload, { timeout })
  ▼
producer.enqueueAndWait()
  │
  ├─ Deferred.make<TResult, JobFailedError>()
  │
  ├─ storage.subscribeToJob(id, callback)    ◄── subscribe BEFORE enqueue to avoid race
  │     callback("completed") → fetch result → Deferred.succeed(deferred, result)
  │     callback("failed")    → fetch error  → Deferred.fail(deferred, JobFailedError)
  │
  ├─ producer.enqueue(id, payload, opts)
  │     ├─ status "completed" → unsubscribe; return cached result immediately
  │     └─ status "duplicate" + "failed" → unsubscribe; fail with JobFailedError
  │
  ├─ Deferred.await(deferred)
  │     .pipe(Effect.timeout(timeout millis))
  │     .pipe(Effect.mapError(TimeoutException → TimeoutError))
  │
  └─ (finally) storage.unsubscribeFromJob(id)
```

### Consumer Loop

Each worker fiber runs this loop concurrently (one fiber per `concurrency` slot):

```
Effect.forever(
  │
  ├─ Ref.get(handlerRef) → null? → sleep 100ms → next iteration
  │
  ├─ storage.dequeue(workerId, blockTimeout)
  │     blocks up to blockTimeout seconds if queue is empty
  │     atomically moves message → worker processing queue
  │
  └─ processJob(message, handler)
       │
       ├─ payloadSerde.deserialize(message) → QueueMessage
       ├─ storage.getJobState(id) == null? → ack (cancelled job), return
       │
       ├─ storage.setJobState(id, "processing:ts:workerId")
       ├─ storage.publishEvent(id, "processing")   ← Reaper picks this up
       │
       ├─ AbortController created (signal passed to job)
       │
       ├─ handler(job)
       │     .pipe(Effect.timeout(visibilityTimeout millis))
       │     .pipe(Effect.tapErrorCause → abort signal)
       │     .pipe(Effect.exit)
       │
       ├─ afterExecution hook (sync or async) mutates ctx.ttl / ctx.result / ctx.error
       │
       ├─ Success:
       │     storage.completeJob(id, msg, workerId, result, ttl)
       │       → jobs[id] = "completed:ts", expiry set
       │       → results[id] = serialized result
       │       → remove from processing queue
       │       → notify "completed" (wakes enqueueAndWait Deferred)
       │       → publish event "completed" (wakes Reaper timer cancellation)
       │     emit QueueEvent { _tag: "completed" }
       │
       └─ Failure:
             attempt < maxAttempts:
               storage.retryJob(id, updatedMsg, workerId, attempt)
                 → jobs[id] = "failing:ts:attempt"
                 → requeue message (push back onto main queue)
                 → notify "failing"
                 → publish event "failing"
               emit QueueEvent { _tag: "failing" }

             attempt == maxAttempts:
               storage.failJob(id, msg, workerId, serializedError, ttl)
                 → jobs[id] = "failed:ts", expiry set
                 → errors[id] = serialized error JSON
                 → remove from processing queue
                 → notify "failed"  (wakes enqueueAndWait Deferred)
                 → publish event "failed"
               emit QueueEvent { _tag: "failed" }
)
```

### Reaper (stall recovery)

The Reaper watches for jobs that have been in `processing` state longer than
`visibilityTimeout` milliseconds and requeues them so they can be retried by
another worker.

```
makeReaper()
  │
  ├─ On start (no leader election):
  │     becomeActive()
  │       ├─ storage.subscribeToEvents(handleEvent)
  │       └─ checkStalledJobs()         ← initial scan
  │
  ├─ On start (leader election enabled):
  │     tryAcquireLock() → leader?
  │       yes → transitionToLeader() → becomeActive()
  │       no  → start leadershipLoop (retry every acquireRetryInterval)
  │
  ├─ handleEvent(id, event):
  │     "processing"                    → startTimer(id, visibilityTimeout)
  │     "completed"|"failed"|"cancelled" → cancelTimer(id)
  │
  ├─ startTimer(id, delayMs):
  │     fork daemon fiber: sleep(delayMs) → checkJob(id)
  │
  ├─ checkJob(id):
  │     storage.getJobState(id) → still "processing"?
  │       elapsed < visibilityTimeout → restart timer for remaining time
  │       elapsed ≥ visibilityTimeout → recoverStalledJob(id, workerId)
  │
  ├─ recoverStalledJob(id, workerId):
  │     find message in worker's processing queue
  │     storage.requeue(id, message, workerId)  ← put back on main queue
  │     storage.setJobState(id, "failing:ts:attempt")
  │     emit ReaperEvent { _tag: "stalled", id }
  │
  └─ checkStalledJobs() (startup scan):
       for each registered worker:
         for each message in worker's processing queue:
           parse state → still "processing"?
             elapsed ≥ visibilityTimeout → recoverStalledJob immediately
             elapsed <  visibilityTimeout → startTimer for remaining time
```

---

## Storage Abstraction

### StorageShape Interface

`StorageShape` (`src/storage/service.ts`) is the contract every backend must
implement. All methods return `Effect.Effect<A>` with no typed error channel —
storage failures are treated as unrecoverable infrastructure errors (defects).

```
LIFECYCLE
  connect()          — open connection / run schema migrations
  disconnect()       — close connection

QUEUE
  enqueue(id, msg, ts)         — atomic insert-if-not-exists; returns existing state or null
  dequeue(workerId, timeoutSec) — blocking pop; returns message or null on timeout
  requeue(id, msg, workerId)    — move from processing queue back to main queue
  ack(id, msg, workerId)        — remove from processing queue (job finished)

JOB STATE
  getJobState(id)              — returns "status:ts[:extra]" or null
  setJobState(id, state)       — overwrite state (no expiry logic)
  deleteJob(id)                — remove from registry (cancel)
  getJobStates(ids[])          — batch fetch
  setJobExpiry(id, ttlMs)      — set TTL-based expiry on a job's state entry

RESULTS
  setResult(id, buf, ttlMs)    — store serialized result with TTL
  getResult(id)                — fetch result (null if expired)
  setError(id, buf, ttlMs)     — store serialized error with TTL
  getError(id)                 — fetch error (null if expired)

WORKERS
  registerWorker(workerId, ttlMs)   — heartbeat / register worker
  refreshWorker(workerId, ttlMs)    — renew worker TTL
  unregisterWorker(workerId)        — remove worker on graceful stop
  getWorkers()                      — list active (non-expired) workers
  getProcessingJobs(workerId)       — list messages in worker's in-flight queue

NOTIFICATIONS (per-job, for enqueueAndWait)
  subscribeToJob(id, handler)   — register callback for completed/failed/failing
  notifyJobComplete(id, status) — fire registered callbacks for a specific job

EVENTS (all-jobs broadcast, for Reaper)
  subscribeToEvents(handler)    — register callback for all job state changes
  publishEvent(id, event)       — broadcast a state-change to all subscribers

ATOMIC OPERATIONS (compose multiple steps into one)
  completeJob(id, msg, workerId, result, ttl)
  failJob(id, msg, workerId, error, ttl)
  retryJob(id, msg, workerId, attempt)

LEADER ELECTION (optional, for Reaper HA)
  acquireLeaderLock?(lockKey, ownerId, ttlMs) → boolean
  renewLeaderLock?(lockKey, ownerId, ttlMs)   → boolean
  releaseLeaderLock?(lockKey, ownerId)        → boolean
```

The `Storage` class is a `Context.Tag`, which means any Effect that requires
a storage backend simply has `Storage` in its requirements type (`R`). You
satisfy it by `Effect.provide`-ing the appropriate `Layer`.

### In-Memory Backend

`MemoryStorageLive` (`src/storage/memory.ts`) is implemented entirely with
Effect primitives and has no external dependencies:

| Concern | Mechanism |
|---|---|
| Main queue | `Effect.Queue.unbounded<Buffer>()` — single FIFO queue |
| Per-worker processing queues | `Ref<Map<workerId, Buffer[]>>` |
| Job state registry | `Ref<Map<id, stateString>>` |
| TTL-based expiry | `Ref<Map<id, expiresAt>>` — checked lazily on each access |
| Results / errors | `Ref<Map<id, { data, expiresAt }>>` — lazy expiry |
| Workers | `Ref<Map<workerId, { expiresAt }>>` |
| Per-job notifications | `Ref<Map<id, Array<callback>>>` |
| Global event listeners | `Ref<Array<callback>>` |

Blocking `dequeue` is implemented with `Effect.race(Queue.take, sleep(timeout))`.

The in-memory backend does **not** implement leader-election methods, so using
`leaderElection: { enabled: true }` with this backend will emit an error event.

### PostgreSQL Backend

`makePgStorageLayer` (`src/storage/postgres.ts`) uses `@effect/sql-pg` and
creates seven tables (all prefixed, default prefix `"ej"`):

| Table | Purpose |
|---|---|
| `ej_jobs` | Job state registry (`id`, `state`, `expires_at`) |
| `ej_queue` | Pending job messages (`id`, `message`, `enqueued_at`) |
| `ej_processing` | Per-worker in-flight messages (`worker_id`, `job_id`, `message`) |
| `ej_results` | Completed job results (`id`, `data`, `expires_at`) |
| `ej_errors` | Failed job error blobs (`id`, `data`, `expires_at`) |
| `ej_workers` | Active worker heartbeats (`worker_id`, `expires_at`) |
| `ej_leader_locks` | Advisory lock for Reaper leader election (`lock_key`, `owner_id`, `expires_at`) |

Key implementation notes:

- **Atomic dequeue** uses `SELECT … FOR UPDATE SKIP LOCKED` inside a
  transaction so multiple workers cannot pick the same job.
- **Notifications** use PostgreSQL `LISTEN`/`NOTIFY` via `PgClient.listen`
  and a `pgNotify` helper (`SELECT pg_notify($1, $2)`) rather than the
  `@effect/sql-pg` `pg.notify()` helper (which generates invalid syntax).
- **In-process fan-out** is handled by an Effect `PubSub` so multiple
  `enqueueAndWait` callers waiting on the same job each receive the notification.
- **Schema migration** runs on every `connect()` call using `CREATE TABLE IF NOT EXISTS`.
- **Leader election** is table-based: `ej_leader_locks` rows carry an
  `expires_at` timestamp; `INSERT … ON CONFLICT DO UPDATE` combined with a
  freshness check provides atomic acquire/renew semantics.
- **Sql errors** are converted to defects (`Effect.orDie`) — the storage layer
  never surfaces typed errors to the queue or reaper logic.

---

## Effect Patterns

| Pattern | Where used | Why |
|---|---|---|
| `Context.Tag` | `Storage` | Zero-cost service injection; swap backends by changing `Layer` |
| `Layer.effect` | `MemoryStorageLive`, `PgStorageLive` | Effectful construction of stateful services |
| `Effect.gen` | Everywhere | Imperative-style composition with full type inference |
| `Ref` | Worker fibers, timer map, handler ref, started flag | Mutable state inside pure Effects |
| `Fiber` | Worker loops, reaper timers, leadership loop | Structured concurrency — fibers tracked in `Ref` and interrupted on stop |
| `EQueue.unbounded` | Main queue (memory), event queue | Async back-pressure-free communication between fibers |
| `Deferred` | `enqueueAndWait` | One-shot promise for request/response flow |
| `Stream.fromQueue` | `QueueHandle.events`, `Reaper.events` | Expose internal queue as a pull-based event stream |
| `Effect.scope` + `Effect.addFinalizer` | `makeQueue` | Automatic `stop()` when the enclosing `Scope` closes |
| `Effect.forkDaemon` | Worker loops, reaper timers | Run long-lived fibers that survive parent fiber completion |
| `Effect.forever` | Worker loop | Repeat-forever with early-exit via fiber interruption |
| `Effect.exit` | Job execution | Capture both typed errors and defects as `Exit` without terminating the fiber |
| `Data.TaggedError` | All error types | Nominal typed errors with discriminant `_tag` for `Effect.catchTag` |
| `Effect.timeout` | Job execution, `enqueueAndWait` | Deadline enforcement without external timers |
| `Effect.race` | Memory `dequeue` | Blocking wait with timeout |
| `Effect.provideService` | Producer methods | Inject `Storage` into Effects that need it without propagating the requirement to callers |

---

## Error Taxonomy

All errors extend `Data.TaggedError` and carry a `_tag` discriminant for
pattern matching with `Effect.catchTag` / `Effect.catchTags`.

| Error | `_tag` | Thrown by |
|---|---|---|
| `TimeoutError` | `"TimeoutError"` | `enqueueAndWait` when job does not complete within `timeout` ms |
| `MaxRetriesError` | `"MaxRetriesError"` | Consumer when a job exhausts all retry attempts |
| `JobFailedError` | `"JobFailedError"` | `enqueueAndWait` when the job it is waiting on has failed |
| `JobNotFoundError` | `"JobNotFoundError"` | Reserved; not currently thrown in public API |
| `JobCancelledError` | `"JobCancelledError"` | Reserved; not currently thrown in public API |
| `StorageError` | `"StorageError"` | Reserved; storage defects bypass typed errors |
| `InvalidResultTTLError` | `"InvalidResultTTLError"` | `enqueue` / `enqueueAndWait` / `updateResultTTL` when TTL ≤ 0 |
| `JobQueueError` | `"JobQueueError"` | Base class; not thrown directly |

---

## Dependency Graph

```
                    ┌─────────────────────────────────────────────────────────┐
                    │                    makeQueue()                          │
                    │                                                         │
                    │   ┌──────────────┐          ┌──────────────────────┐   │
                    │   │  makeProducer │          │    makeConsumer       │   │
                    │   │              │          │                      │   │
                    │   │  enqueue     │          │  workerLoop (fiber)   │   │
                    │   │  enqueueAnd  │          │  processJob           │   │
                    │   │  Wait        │          │  afterExecution hook  │   │
                    │   │  cancel      │          │                      │   │
                    │   │  getResult   │          │   ┌──────────────┐   │   │
                    │   │  updateTTL   │          │   │  handler ref │   │   │
                    │   │  getStatus   │          │   └──────────────┘   │   │
                    │   └──────┬───────┘          └──────────┬───────────┘   │
                    │          │                             │               │
                    └──────────┼─────────────────────────────┼───────────────┘
                               │                             │
                               ▼                             ▼
                    ┌─────────────────────────────────────────────────────────┐
                    │                   Storage (Context.Tag)                 │
                    │                                                         │
                    │           StorageShape interface                        │
                    └─────────────────┬─────────────────────────┬────────────┘
                                      │                         │
                       ┌──────────────┘                         └─────────────────┐
                       ▼                                                          ▼
          ┌───────────────────────┐                              ┌───────────────────────────┐
          │  MemoryStorageLive    │                              │  makePgStorageLayer()      │
          │  (Layer<Storage>)     │                              │  (Layer<Storage, SqlError, │
          │                      │                              │   PgClient | SqlClient>)   │
          │  Effect.Queue + Refs  │                              │                           │
          └───────────────────────┘                              │  @effect/sql-pg           │
                                                                 │  LISTEN/NOTIFY/PubSub     │
                                                                 └───────────────────────────┘


                    ┌─────────────────────────────────────────────────────────┐
                    │                    makeReaper()                         │
                    │                                                         │
                    │  subscribeToEvents  ──────────────────────────────────► │
                    │  startTimer / cancelTimer (Fiber map in Ref)            │ Storage
                    │  checkStalledJobs (startup scan)                        │ (same tag)
                    │  leader election loop (optional)                        │
                    └─────────────────────────────────────────────────────────┘
```

`makeQueue` and `makeReaper` both require the same `Storage` tag in their
Effect context — they share the same storage backend instance when provided
through the same `Layer`. This means the Reaper can observe and requeue jobs
that were enqueued or are being processed by any queue that uses the same
backend.

The typical Layer composition looks like:

```ts
import { Effect, Layer } from "effect"
import { makeQueue, makeReaper, MemoryStorage } from "effective-job"

const program = Effect.gen(function* () {
  const queue  = yield* makeQueue<Payload, Result>({ concurrency: 4 })
  const reaper = yield* makeReaper({ visibilityTimeout: 30_000 })

  yield* queue.execute(handler)
  yield* queue.start
  yield* reaper.start

  // ...

  yield* queue.stop
  yield* reaper.stop
})

// Both queue and reaper share the same MemoryStorage instance
Effect.runPromise(Effect.scoped(program).pipe(Effect.provide(MemoryStorage)))
```
