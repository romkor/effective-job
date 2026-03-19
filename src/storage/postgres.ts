import { Effect, Layer, PubSub, Queue as EQueue, Ref, Stream } from "effect"
import { PgClient } from "@effect/sql-pg/PgClient"
import { SqlClient } from "@effect/sql/SqlClient"
import type { SqlError } from "@effect/sql/SqlError"
import { Storage } from "./service.ts"

// ── Config ─────────────────────────────────────────────────────────────────────

/**
 * Configuration for the PostgreSQL storage backend.
 *
 * All tables are prefixed with `tablePrefix` so multiple queues can share one
 * database schema without name collisions.
 */
export interface PgStorageConfig {
  /** Prefix for all table names. Defaults to `"ej"`. */
  readonly tablePrefix?: string | undefined
}

// ── Helpers ────────────────────────────────────────────────────────────────────

/**
 * Convert any `SqlError` into a fiber defect.
 *
 * Storage backends are infrastructure – if the database is down the worker
 * fiber should die rather than surface a typed error to application code.
 * Callers (consumer, producer) already catch defects via `catchAllCause`.
 */
const db = <A>(e: Effect.Effect<A, SqlError>): Effect.Effect<A> => Effect.orDie(e)

// ── Layer ──────────────────────────────────────────────────────────────────────

/**
 * Build a PostgreSQL-backed Storage Layer.
 *
 * Requires both `PgClient` and `SqlClient` in the context; both are provided
 * by `PgClient.layer(config)` from `@effect/sql-pg`.
 *
 * Uses:
 * - SQL queries via `@effect/sql`'s tagged template API (`SqlClient`)
 * - `LISTEN`/`NOTIFY` via `PgClient.listen` / `PgClient.notify`
 * - `SELECT … FOR UPDATE SKIP LOCKED` for atomic dequeue
 * - A table-based advisory lock for optional leader election
 *
 * @example
 * ```ts
 * import { Layer, Redacted } from "effect"
 * import { PgClient } from "@effect/sql-pg/PgClient"
 * import { makePgStorageLayer } from "effective-job"
 *
 * const StorageLayer = makePgStorageLayer().pipe(
 *   Layer.provide(PgClient.layer({ url: Redacted.make(process.env.DATABASE_URL!) }))
 * )
 * ```
 */
export const makePgStorageLayer = (
  config: PgStorageConfig = {}
): Layer.Layer<Storage, SqlError, PgClient | SqlClient> =>
  Layer.effect(
    Storage,
    Effect.gen(function* () {
      const p = config.tablePrefix ?? "ej"

      // Use withoutTransforms() so column names are returned as-is (snake_case)
      const sql = (yield* SqlClient).withoutTransforms()
      const pg = yield* PgClient

      // ── Table / index creation ─────────────────────────────────────────

      const jobsTable = sql(p + "_jobs")
      const queueTable = sql(p + "_queue")
      const processingTable = sql(p + "_processing")
      const resultsTable = sql(p + "_results")
      const errorsTable = sql(p + "_errors")
      const workersTable = sql(p + "_workers")
      const locksTable = sql(p + "_leader_locks")

      yield* sql`CREATE TABLE IF NOT EXISTS ${jobsTable} (
        id          TEXT    PRIMARY KEY,
        state       TEXT    NOT NULL,
        expires_at  BIGINT
      )`

      yield* sql`CREATE TABLE IF NOT EXISTS ${queueTable} (
        id           TEXT   PRIMARY KEY,
        message      BYTEA  NOT NULL,
        enqueued_at  BIGINT NOT NULL
      )`

      yield* sql`CREATE INDEX IF NOT EXISTS ${sql(p + "_queue_order")}
        ON ${queueTable} (enqueued_at ASC)`

      yield* sql`CREATE TABLE IF NOT EXISTS ${processingTable} (
        worker_id  TEXT  NOT NULL,
        job_id     TEXT  NOT NULL,
        message    BYTEA NOT NULL,
        PRIMARY KEY (worker_id, job_id)
      )`

      yield* sql`CREATE TABLE IF NOT EXISTS ${resultsTable} (
        id          TEXT   PRIMARY KEY,
        data        BYTEA  NOT NULL,
        expires_at  BIGINT NOT NULL
      )`

      yield* sql`CREATE TABLE IF NOT EXISTS ${errorsTable} (
        id          TEXT   PRIMARY KEY,
        data        BYTEA  NOT NULL,
        expires_at  BIGINT NOT NULL
      )`

      yield* sql`CREATE TABLE IF NOT EXISTS ${workersTable} (
        worker_id   TEXT   PRIMARY KEY,
        expires_at  BIGINT NOT NULL
      )`

      yield* sql`CREATE TABLE IF NOT EXISTS ${locksTable} (
        lock_key    TEXT   PRIMARY KEY,
        owner_id    TEXT   NOT NULL,
        expires_at  BIGINT NOT NULL
      )`

      // ── NOTIFY channel names ───────────────────────────────────────────

      const newJobChannel = p + "_new_job"
      const eventsChannel = p + "_events"
      const notifyChannel = p + "_notify"

      // ── In-process state ───────────────────────────────────────────────

      /** PubSub: signals waiting `dequeue` callers when a new job is enqueued */
      const newJobHub = yield* PubSub.unbounded<void>()

      /** In-process handlers registered via `subscribeToEvents` (for Reaper) */
      const eventHandlers = yield* Ref.make<Array<(id: string, event: string) => void>>([])

      /** In-process handlers registered via `subscribeToJob` (for `enqueueAndWait`) */
      const notifyHandlers = yield* Ref.make(
        new Map<string, Array<(status: "completed" | "failed" | "failing") => void>>()
      )

      /**
       * Send a PostgreSQL NOTIFY using `pg_notify()` which supports parameterized
       * arguments (unlike the `NOTIFY channel, $1` syntax which is invalid in PG).
       */
      const pgNotify = (channel: string, payload: string): Effect.Effect<void, SqlError> =>
        sql`SELECT pg_notify(${channel}, ${payload})`.pipe(Effect.as(undefined as void))

      // ── Background LISTEN fibers ───────────────────────────────────────

      // Wake up waiting dequeue callers whenever a new job is enqueued
      yield* pg.listen(newJobChannel).pipe(
        Stream.mapEffect(() => PubSub.publish(newJobHub, undefined as void)),
        Stream.runDrain,
        Effect.forkDaemon
      )

      // Fan-out job-state events to Reaper / monitoring handlers
      yield* pg.listen(eventsChannel).pipe(
        Stream.mapEffect((payload) => {
          const sep = payload.indexOf(":")
          const id = payload.slice(0, sep)
          const event = payload.slice(sep + 1)
          return Ref.get(eventHandlers).pipe(
            Effect.flatMap((hs) => Effect.sync(() => hs.forEach((h) => h(id, event))))
          )
        }),
        Stream.runDrain,
        Effect.forkDaemon
      )

      // Fan-out job completion notifications to `enqueueAndWait` waiters
      yield* pg.listen(notifyChannel).pipe(
        Stream.mapEffect((payload) => {
          const sep = payload.indexOf(":")
          const id = payload.slice(0, sep)
          const status = payload.slice(sep + 1) as "completed" | "failed" | "failing"
          return Ref.get(notifyHandlers).pipe(
            Effect.flatMap((map) =>
              Effect.sync(() => map.get(id)?.forEach((h) => h(status)))
            )
          )
        }),
        Stream.runDrain,
        Effect.forkDaemon
      )

      // ── Atomic dequeue helper ──────────────────────────────────────────

      /**
       * Non-blocking: atomically move the oldest queued job into the given
       * worker's processing table using `SELECT … FOR UPDATE SKIP LOCKED`.
       */
      const tryDequeue = (workerId: string): Effect.Effect<Buffer | null, SqlError> =>
        sql.withTransaction(
          Effect.gen(function* () {
            const rows = yield* sql<{ message: Uint8Array }>`
              WITH dequeued AS (
                DELETE FROM ${queueTable}
                WHERE id = (
                  SELECT id FROM ${queueTable}
                  ORDER BY enqueued_at ASC
                  LIMIT 1
                  FOR UPDATE SKIP LOCKED
                )
                RETURNING id, message
              )
              INSERT INTO ${processingTable} (worker_id, job_id, message)
              SELECT ${workerId}, id, message FROM dequeued
              RETURNING message
            `
            const row = rows[0]
            return row ? Buffer.from(row.message) : null
          })
        )

      // ── Storage implementation ─────────────────────────────────────────

      const connect = (): Effect.Effect<void> => Effect.void

      const disconnect = (): Effect.Effect<void> =>
        Effect.gen(function* () {
          yield* Ref.set(eventHandlers, [])
          yield* Ref.set(notifyHandlers, new Map())
        })

      const enqueue = (id: string, message: Buffer, timestamp: number): Effect.Effect<string | null> =>
        db(
          sql.withTransaction(
            Effect.gen(function* () {
              const now = Date.now()

              // Check for a non-expired existing job (with a row lock to prevent races)
              const existing = yield* sql<{ state: string }>`
                SELECT state FROM ${jobsTable}
                WHERE id = ${id}
                AND (expires_at IS NULL OR expires_at > ${now})
                FOR UPDATE
              `
              const existingRow = existing[0]
              if (existingRow !== undefined) {
                return existingRow.state // duplicate – return current state
              }

              // Remove any stale/expired entry so we can re-insert
              yield* sql`DELETE FROM ${jobsTable} WHERE id = ${id}`

              yield* sql`
                INSERT INTO ${jobsTable} (id, state, expires_at)
                VALUES (${id}, ${"queued:" + timestamp}, NULL)
              `
              yield* sql`
                INSERT INTO ${queueTable} (id, message, enqueued_at)
                VALUES (${id}, ${message}, ${timestamp})
              `

              yield* pgNotify(newJobChannel, "")
              yield* pgNotify(eventsChannel, id + ":queued")

              return null
            })
          )
        )

      const dequeue = (workerId: string, timeoutSeconds: number): Effect.Effect<Buffer | null> =>
        db(
          Effect.gen(function* () {
            // Fast path: non-blocking attempt
            const immediate = yield* tryDequeue(workerId)
            if (immediate !== null) return immediate

            const deadline = Date.now() + timeoutSeconds * 1_000

            // Slow path: subscribe to new-job PubSub, retry until timeout
            return yield* Effect.scoped(
              Effect.gen(function* () {
                const sub = yield* PubSub.subscribe(newJobHub)

                const loop: Effect.Effect<Buffer | null, SqlError> = Effect.gen(function* () {
                  const remaining = deadline - Date.now()
                  if (remaining <= 0) return null

                  // Await either a notification or at most 5 s (handles missed NOTIFYs)
                  yield* Effect.race(
                    EQueue.take(sub).pipe(Effect.as(undefined as void)),
                    Effect.sleep(`${Math.min(remaining, 5_000)} millis`)
                  )

                  const r = yield* tryDequeue(workerId)
                  if (r !== null) return r
                  return yield* loop
                })

                return yield* loop
              })
            )
          })
        )

      const requeue = (id: string, message: Buffer, workerId: string): Effect.Effect<void> =>
        db(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* sql`DELETE FROM ${processingTable} WHERE worker_id = ${workerId} AND job_id = ${id}`
              yield* sql`
                INSERT INTO ${queueTable} (id, message, enqueued_at)
                VALUES (${id}, ${message}, ${Date.now()})
                ON CONFLICT (id) DO NOTHING
              `
              yield* pgNotify(newJobChannel, "")
            })
          )
        )

      const ack = (id: string, _message: Buffer, workerId: string): Effect.Effect<void> =>
        db(sql`DELETE FROM ${processingTable} WHERE worker_id = ${workerId} AND job_id = ${id}`)

      const getJobState = (id: string): Effect.Effect<string | null> =>
        db(
          Effect.gen(function* () {
            const now = Date.now()
            const rows = yield* sql<{ state: string }>`
              SELECT state FROM ${jobsTable}
              WHERE id = ${id}
              AND (expires_at IS NULL OR expires_at > ${now})
            `
            return rows[0]?.state ?? null
          })
        )

      const setJobState = (id: string, state: string): Effect.Effect<void> =>
        db(sql`UPDATE ${jobsTable} SET state = ${state} WHERE id = ${id}`)

      const deleteJob = (id: string): Effect.Effect<boolean> =>
        db(
          Effect.gen(function* () {
            const rows = yield* sql<{ id: string }>`
              DELETE FROM ${jobsTable} WHERE id = ${id} RETURNING id
            `
            if (rows.length > 0) {
              yield* pgNotify(eventsChannel, id + ":cancelled")
            }
            return rows.length > 0
          })
        )

      const getJobStates = (ids: ReadonlyArray<string>): Effect.Effect<Map<string, string | null>> =>
        db(
          Effect.gen(function* () {
            const result = new Map<string, string | null>()
            for (const id of ids) {
              result.set(id, yield* getJobState(id))
            }
            return result
          })
        )

      const setJobExpiry = (id: string, ttlMs: number): Effect.Effect<void> => {
        const expiresAt = Date.now() + ttlMs
        return db(sql`UPDATE ${jobsTable} SET expires_at = ${expiresAt} WHERE id = ${id}`)
      }

      const setResult = (id: string, data: Buffer, ttlMs: number): Effect.Effect<void> => {
        const expiresAt = Date.now() + ttlMs
        return db(sql`
          INSERT INTO ${resultsTable} (id, data, expires_at) VALUES (${id}, ${data}, ${expiresAt})
          ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, expires_at = EXCLUDED.expires_at
        `)
      }

      const getResult = (id: string): Effect.Effect<Buffer | null> =>
        db(
          Effect.gen(function* () {
            const now = Date.now()
            const rows = yield* sql<{ data: Uint8Array }>`
              SELECT data FROM ${resultsTable} WHERE id = ${id} AND expires_at > ${now}
            `
            const row = rows[0]
            return row ? Buffer.from(row.data) : null
          })
        )

      const setError = (id: string, data: Buffer, ttlMs: number): Effect.Effect<void> => {
        const expiresAt = Date.now() + ttlMs
        return db(sql`
          INSERT INTO ${errorsTable} (id, data, expires_at) VALUES (${id}, ${data}, ${expiresAt})
          ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, expires_at = EXCLUDED.expires_at
        `)
      }

      const getError = (id: string): Effect.Effect<Buffer | null> =>
        db(
          Effect.gen(function* () {
            const now = Date.now()
            const rows = yield* sql<{ data: Uint8Array }>`
              SELECT data FROM ${errorsTable} WHERE id = ${id} AND expires_at > ${now}
            `
            const row = rows[0]
            return row ? Buffer.from(row.data) : null
          })
        )

      const registerWorker = (workerId: string, ttlMs: number): Effect.Effect<void> => {
        const expiresAt = Date.now() + ttlMs
        return db(sql`
          INSERT INTO ${workersTable} (worker_id, expires_at) VALUES (${workerId}, ${expiresAt})
          ON CONFLICT (worker_id) DO UPDATE SET expires_at = EXCLUDED.expires_at
        `)
      }

      const refreshWorker = (workerId: string, ttlMs: number): Effect.Effect<void> =>
        registerWorker(workerId, ttlMs)

      const unregisterWorker = (workerId: string): Effect.Effect<void> =>
        db(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* sql`DELETE FROM ${workersTable} WHERE worker_id = ${workerId}`
              yield* sql`DELETE FROM ${processingTable} WHERE worker_id = ${workerId}`
            })
          )
        )

      const getWorkers = (): Effect.Effect<ReadonlyArray<string>> =>
        db(
          Effect.gen(function* () {
            const now = Date.now()
            const rows = yield* sql<{ worker_id: string }>`
              SELECT worker_id FROM ${workersTable} WHERE expires_at > ${now}
            `
            return rows.map((r) => r.worker_id)
          })
        )

      const getProcessingJobs = (workerId: string): Effect.Effect<ReadonlyArray<Buffer>> =>
        db(
          Effect.gen(function* () {
            const rows = yield* sql<{ message: Uint8Array }>`
              SELECT message FROM ${processingTable} WHERE worker_id = ${workerId}
            `
            return rows.map((r) => Buffer.from(r.message))
          })
        )

      const subscribeToJob = (
        id: string,
        handler: (status: "completed" | "failed" | "failing") => void
      ): Effect.Effect<Effect.Effect<void>> =>
        Effect.gen(function* () {
          yield* Ref.update(notifyHandlers, (map) => {
            const hs = map.get(id) ?? []
            hs.push(handler)
            map.set(id, hs)
            return map
          })
          return Ref.update(notifyHandlers, (map) => {
            const hs = map.get(id)
            if (hs) {
              const idx = hs.indexOf(handler)
              if (idx !== -1) hs.splice(idx, 1)
            }
            return map
          })
        })

      const notifyJobComplete = (
        id: string,
        status: "completed" | "failed" | "failing"
      ): Effect.Effect<void> =>
        db(pgNotify(notifyChannel, id + ":" + status))

      const subscribeToEvents = (
        handler: (id: string, event: string) => void
      ): Effect.Effect<Effect.Effect<void>> =>
        Effect.gen(function* () {
          yield* Ref.update(eventHandlers, (hs) => { hs.push(handler); return hs })
          return Ref.update(eventHandlers, (hs) => {
            const idx = hs.indexOf(handler)
            if (idx !== -1) hs.splice(idx, 1)
            return hs
          })
        })

      const publishEvent = (id: string, event: string): Effect.Effect<void> =>
        db(pgNotify(eventsChannel, id + ":" + event))

      // ── Atomic composite operations ────────────────────────────────────

      const completeJob = (
        id: string,
        _message: Buffer,
        workerId: string,
        result: Buffer,
        resultTTL: number
      ): Effect.Effect<void> =>
        db(
          sql.withTransaction(
            Effect.gen(function* () {
              const ts = Date.now()
              const expiresAt = ts + resultTTL
              yield* sql`
                UPDATE ${jobsTable} SET state = ${"completed:" + ts}, expires_at = ${expiresAt}
                WHERE id = ${id}
              `
              yield* sql`
                INSERT INTO ${resultsTable} (id, data, expires_at) VALUES (${id}, ${result}, ${expiresAt})
                ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, expires_at = EXCLUDED.expires_at
              `
              yield* sql`DELETE FROM ${processingTable} WHERE worker_id = ${workerId} AND job_id = ${id}`
              yield* pgNotify(notifyChannel, id + ":completed")
              yield* pgNotify(eventsChannel, id + ":completed")
            })
          )
        )

      const failJob = (
        id: string,
        _message: Buffer,
        workerId: string,
        error: Buffer,
        errorTTL: number
      ): Effect.Effect<void> =>
        db(
          sql.withTransaction(
            Effect.gen(function* () {
              const ts = Date.now()
              const expiresAt = ts + errorTTL
              yield* sql`
                UPDATE ${jobsTable} SET state = ${"failed:" + ts}, expires_at = ${expiresAt}
                WHERE id = ${id}
              `
              yield* sql`
                INSERT INTO ${errorsTable} (id, data, expires_at) VALUES (${id}, ${error}, ${expiresAt})
                ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, expires_at = EXCLUDED.expires_at
              `
              yield* sql`DELETE FROM ${processingTable} WHERE worker_id = ${workerId} AND job_id = ${id}`
              yield* pgNotify(notifyChannel, id + ":failed")
              yield* pgNotify(eventsChannel, id + ":failed")
            })
          )
        )

      const retryJob = (
        id: string,
        message: Buffer,
        workerId: string,
        attempts: number
      ): Effect.Effect<void> =>
        db(
          sql.withTransaction(
            Effect.gen(function* () {
              const ts = Date.now()
              yield* sql`
                UPDATE ${jobsTable} SET state = ${"failing:" + ts + ":" + attempts}
                WHERE id = ${id}
              `
              yield* sql`DELETE FROM ${processingTable} WHERE worker_id = ${workerId} AND job_id = ${id}`
              yield* sql`
                INSERT INTO ${queueTable} (id, message, enqueued_at)
                VALUES (${id}, ${message}, ${ts})
                ON CONFLICT (id) DO NOTHING
              `
              yield* pgNotify(newJobChannel, "")
              yield* pgNotify(notifyChannel, id + ":failing")
              yield* pgNotify(eventsChannel, id + ":failing")
            })
          )
        )

      // ── Leader election (table-based, TTL via expires_at) ──────────────

      const acquireLeaderLock = (lockKey: string, ownerId: string, ttlMs: number): Effect.Effect<boolean> =>
        db(
          sql.withTransaction(
            Effect.gen(function* () {
              const now = Date.now()
              // Remove any expired lock so we can insert
              yield* sql`DELETE FROM ${locksTable} WHERE lock_key = ${lockKey} AND expires_at < ${now}`
              // Try to insert; if we already own the lock, update the TTL instead.
              // ON CONFLICT DO UPDATE WHERE restricts the update to the case where
              // we're already the owner → 0 rows returned if someone else holds it.
              const rows = yield* sql<{ owner_id: string }>`
                INSERT INTO ${locksTable} (lock_key, owner_id, expires_at)
                VALUES (${lockKey}, ${ownerId}, ${now + ttlMs})
                ON CONFLICT (lock_key) DO UPDATE
                  SET expires_at = EXCLUDED.expires_at
                WHERE ${locksTable}.owner_id = ${ownerId}
                RETURNING owner_id
              `
              return rows.length > 0
            })
          )
        )

      const renewLeaderLock = (lockKey: string, ownerId: string, ttlMs: number): Effect.Effect<boolean> =>
        db(
          Effect.gen(function* () {
            const now = Date.now()
            const rows = yield* sql<{ owner_id: string }>`
              UPDATE ${locksTable}
              SET expires_at = ${now + ttlMs}
              WHERE lock_key = ${lockKey} AND owner_id = ${ownerId}
              RETURNING owner_id
            `
            return rows.length > 0
          })
        )

      const releaseLeaderLock = (lockKey: string, ownerId: string): Effect.Effect<boolean> =>
        db(
          Effect.gen(function* () {
            const rows = yield* sql<{ lock_key: string }>`
              DELETE FROM ${locksTable}
              WHERE lock_key = ${lockKey} AND owner_id = ${ownerId}
              RETURNING lock_key
            `
            return rows.length > 0
          })
        )

      return {
        connect,
        disconnect,
        enqueue,
        dequeue,
        requeue,
        ack,
        getJobState,
        setJobState,
        deleteJob,
        getJobStates,
        setJobExpiry,
        setResult,
        getResult,
        setError,
        getError,
        registerWorker,
        refreshWorker,
        unregisterWorker,
        getWorkers,
        getProcessingJobs,
        subscribeToJob,
        notifyJobComplete,
        subscribeToEvents,
        publishEvent,
        completeJob,
        failJob,
        retryJob,
        acquireLeaderLock,
        renewLeaderLock,
        releaseLeaderLock
      }
    })
  )

/**
 * Convenience alias – provides `Storage` using the default `"ej"` table prefix.
 * Requires `PgClient | SqlClient` in the Layer context.
 */
export const PgStorageLive = makePgStorageLayer()
