import { Effect, Layer, Queue as EQueue, Ref } from "effect"
import { Storage } from "./service.ts"

interface StoredResult {
  data: Buffer
  expiresAt: number
}

interface WorkerInfo {
  expiresAt: number
}

/**
 * In-memory storage Layer for testing and single-process scenarios.
 *
 * Provides the Storage service backed entirely by in-memory data structures.
 * Use `MemoryStorage.live` in your Layer composition.
 */
export const MemoryStorageLive: Layer.Layer<Storage> = Layer.effect(
  Storage,
  Effect.gen(function* () {
    // ── Internal state ─────────────────────────────────────────────
    const mainQueue = yield* EQueue.unbounded<Buffer>()
    const processingQueues = yield* Ref.make(new Map<string, Buffer[]>())
    const jobs = yield* Ref.make(new Map<string, string>())
    const jobExpiry = yield* Ref.make(new Map<string, number>())
    const results = yield* Ref.make(new Map<string, StoredResult>())
    const errors = yield* Ref.make(new Map<string, StoredResult>())
    const workers = yield* Ref.make(new Map<string, WorkerInfo>())

    // Event subscribers (for monitoring / reaper)
    const eventHandlers = yield* Ref.make<Array<(id: string, event: string) => void>>([])
    // Notification subscribers (for enqueueAndWait)
    const notifyHandlers = yield* Ref.make(new Map<string, Array<(status: "completed" | "failed" | "failing") => void>>())

    // ── Helpers ────────────────────────────────────────────────────

    const publishEventInternal = (id: string, event: string): Effect.Effect<void> =>
      Effect.flatMap(Ref.get(eventHandlers), (handlers) =>
        Effect.sync(() => {
          for (const h of handlers) {
            h(id, event)
          }
        })
      )

    const notifyJobInternal = (id: string, status: "completed" | "failed" | "failing"): Effect.Effect<void> =>
      Effect.flatMap(Ref.get(notifyHandlers), (map) =>
        Effect.sync(() => {
          const handlers = map.get(id)
          if (handlers) {
            for (const h of handlers) {
              h(status)
            }
          }
        })
      )

    const checkJobExpiry = (id: string): Effect.Effect<boolean> =>
      Effect.gen(function* () {
        const expiry = yield* Ref.get(jobExpiry)
        const expiresAt = expiry.get(id)
        if (expiresAt !== undefined && Date.now() >= expiresAt) {
          yield* Ref.update(jobs, (m) => { m.delete(id); return m })
          yield* Ref.update(jobExpiry, (m) => { m.delete(id); return m })
          return true // was expired
        }
        return false
      })

    const addToProcessingQueue = (workerId: string, message: Buffer): Effect.Effect<void> =>
      Ref.update(processingQueues, (map) => {
        const q = map.get(workerId) ?? []
        q.push(message)
        map.set(workerId, q)
        return map
      })

    const removeFromProcessingQueue = (message: Buffer, workerId: string): Effect.Effect<void> =>
      Ref.update(processingQueues, (map) => {
        const q = map.get(workerId)
        if (q) {
          const idx = q.findIndex((m) => m.equals(message))
          if (idx !== -1) q.splice(idx, 1)
        }
        return map
      })

    // ── Storage implementation ─────────────────────────────────────

    const connect = (): Effect.Effect<void> => Effect.void

    const disconnect = (): Effect.Effect<void> =>
      Effect.gen(function* () {
        // Clear subscribers; worker fibers are interrupted by the Queue's stop() before
        // disconnect is called, so there are no pending dequeue() calls to unblock.
        yield* Ref.set(eventHandlers, [])
        yield* Ref.set(notifyHandlers, new Map())
      })

    const enqueue = (id: string, message: Buffer, timestamp: number): Effect.Effect<string | null> =>
      Effect.gen(function* () {
        const jobMap = yield* Ref.get(jobs)
        const existing = jobMap.get(id)

        if (existing !== undefined) {
          const expired = yield* checkJobExpiry(id)
          if (!expired) {
            return existing
          }
        }

        yield* Ref.update(jobs, (m) => { m.set(id, `queued:${timestamp}`); return m })
        yield* EQueue.offer(mainQueue, message)
        yield* publishEventInternal(id, "queued")
        return null
      })

    const dequeue = (workerId: string, timeoutSeconds: number): Effect.Effect<Buffer | null> =>
      Effect.gen(function* () {
        const result = yield* EQueue.poll(mainQueue)
        if (result._tag === "Some") {
          yield* addToProcessingQueue(workerId, result.value)
          return result.value
        }

        // Wait with timeout
        const raceResult = yield* Effect.race(
          EQueue.take(mainQueue),
          Effect.sleep(`${timeoutSeconds} seconds`).pipe(Effect.as(null as Buffer | null))
        )

        if (raceResult !== null) {
          yield* addToProcessingQueue(workerId, raceResult)
        }
        return raceResult
      })

    const requeue = (id: string, message: Buffer, workerId: string): Effect.Effect<void> =>
      Effect.gen(function* () {
        yield* removeFromProcessingQueue(message, workerId)
        // Put back at front – use a new queue offer (FIFO for retry)
        yield* EQueue.offer(mainQueue, message)
      })

    const ack = (_id: string, message: Buffer, workerId: string): Effect.Effect<void> =>
      removeFromProcessingQueue(message, workerId)

    const getJobState = (id: string): Effect.Effect<string | null> =>
      Effect.gen(function* () {
        const jobMap = yield* Ref.get(jobs)
        const state = jobMap.get(id)
        if (state === undefined) return null
        const expired = yield* checkJobExpiry(id)
        if (expired) return null
        return state
      })

    const setJobState = (id: string, state: string): Effect.Effect<void> =>
      Ref.update(jobs, (m) => { m.set(id, state); return m })

    const deleteJob = (id: string): Effect.Effect<boolean> =>
      Effect.gen(function* () {
        const jobMap = yield* Ref.get(jobs)
        const existed = jobMap.has(id)
        if (existed) {
          yield* Ref.update(jobs, (m) => { m.delete(id); return m })
          yield* Ref.update(jobExpiry, (m) => { m.delete(id); return m })
          yield* publishEventInternal(id, "cancelled")
        }
        return existed
      })

    const getJobStates = (ids: ReadonlyArray<string>): Effect.Effect<Map<string, string | null>> =>
      Effect.gen(function* () {
        const result = new Map<string, string | null>()
        for (const id of ids) {
          result.set(id, yield* getJobState(id))
        }
        return result
      })

    const setJobExpiry = (id: string, ttlMs: number): Effect.Effect<void> =>
      Ref.update(jobExpiry, (m) => { m.set(id, Date.now() + ttlMs); return m })

    const setResult = (id: string, result: Buffer, ttlMs: number): Effect.Effect<void> =>
      Ref.update(results, (m) => {
        m.set(id, { data: result, expiresAt: Date.now() + ttlMs })
        return m
      })

    const getResult = (id: string): Effect.Effect<Buffer | null> =>
      Effect.gen(function* () {
        const map = yield* Ref.get(results)
        const stored = map.get(id)
        if (!stored) return null
        if (Date.now() > stored.expiresAt) {
          yield* Ref.update(results, (m) => { m.delete(id); return m })
          return null
        }
        return stored.data
      })

    const setError = (id: string, error: Buffer, ttlMs: number): Effect.Effect<void> =>
      Ref.update(errors, (m) => {
        m.set(id, { data: error, expiresAt: Date.now() + ttlMs })
        return m
      })

    const getError = (id: string): Effect.Effect<Buffer | null> =>
      Effect.gen(function* () {
        const map = yield* Ref.get(errors)
        const stored = map.get(id)
        if (!stored) return null
        if (Date.now() > stored.expiresAt) {
          yield* Ref.update(errors, (m) => { m.delete(id); return m })
          return null
        }
        return stored.data
      })

    const registerWorker = (workerId: string, ttlMs: number): Effect.Effect<void> =>
      Ref.update(workers, (m) => {
        m.set(workerId, { expiresAt: Date.now() + ttlMs })
        return m
      })

    const refreshWorker = (workerId: string, ttlMs: number): Effect.Effect<void> =>
      registerWorker(workerId, ttlMs)

    const unregisterWorker = (workerId: string): Effect.Effect<void> =>
      Effect.gen(function* () {
        yield* Ref.update(workers, (m) => { m.delete(workerId); return m })
        yield* Ref.update(processingQueues, (m) => { m.delete(workerId); return m })
      })

    const getWorkers = (): Effect.Effect<ReadonlyArray<string>> =>
      Effect.gen(function* () {
        const map = yield* Ref.get(workers)
        const now = Date.now()
        const active: string[] = []
        for (const [id, info] of map) {
          if (now <= info.expiresAt) active.push(id)
        }
        return active
      })

    const getProcessingJobs = (workerId: string): Effect.Effect<ReadonlyArray<Buffer>> =>
      Ref.get(processingQueues).pipe(
        Effect.map((m) => m.get(workerId) ?? [])
      )

    const subscribeToJob = (
      id: string,
      handler: (status: "completed" | "failed" | "failing") => void
    ): Effect.Effect<Effect.Effect<void>> =>
      Effect.gen(function* () {
        yield* Ref.update(notifyHandlers, (map) => {
          const handlers = map.get(id) ?? []
          handlers.push(handler)
          map.set(id, handlers)
          return map
        })

        return Ref.update(notifyHandlers, (map) => {
          const handlers = map.get(id)
          if (handlers) {
            const idx = handlers.indexOf(handler)
            if (idx !== -1) handlers.splice(idx, 1)
          }
          return map
        })
      })

    const notifyJobComplete = (id: string, status: "completed" | "failed" | "failing"): Effect.Effect<void> =>
      notifyJobInternal(id, status)

    const subscribeToEvents = (handler: (id: string, event: string) => void): Effect.Effect<Effect.Effect<void>> =>
      Effect.gen(function* () {
        yield* Ref.update(eventHandlers, (handlers) => { handlers.push(handler); return handlers })

        return Ref.update(eventHandlers, (handlers) => {
          const idx = handlers.indexOf(handler)
          if (idx !== -1) handlers.splice(idx, 1)
          return handlers
        })
      })

    const publishEvent = (id: string, event: string): Effect.Effect<void> =>
      publishEventInternal(id, event)

    const completeJob = (
      id: string,
      message: Buffer,
      workerId: string,
      result: Buffer,
      resultTTL: number
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        const timestamp = Date.now()
        yield* Ref.update(jobs, (m) => { m.set(id, `completed:${timestamp}`); return m })
        yield* Ref.update(jobExpiry, (m) => { m.set(id, timestamp + resultTTL); return m })
        yield* setResult(id, result, resultTTL)
        yield* removeFromProcessingQueue(message, workerId)
        yield* notifyJobInternal(id, "completed")
        yield* publishEventInternal(id, "completed")
      })

    const failJob = (
      id: string,
      message: Buffer,
      workerId: string,
      error: Buffer,
      errorTTL: number
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        const timestamp = Date.now()
        yield* Ref.update(jobs, (m) => { m.set(id, `failed:${timestamp}`); return m })
        yield* Ref.update(jobExpiry, (m) => { m.set(id, timestamp + errorTTL); return m })
        yield* setError(id, error, errorTTL)
        yield* removeFromProcessingQueue(message, workerId)
        yield* notifyJobInternal(id, "failed")
        yield* publishEventInternal(id, "failed")
      })

    const retryJob = (id: string, message: Buffer, workerId: string, attempts: number): Effect.Effect<void> =>
      Effect.gen(function* () {
        const timestamp = Date.now()
        yield* Ref.update(jobs, (m) => { m.set(id, `failing:${timestamp}:${attempts}`); return m })
        yield* requeue(id, message, workerId)
        yield* notifyJobInternal(id, "failing")
        yield* publishEventInternal(id, "failing")
      })

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
      retryJob
    }
  })
)

/**
 * Alias for MemoryStorageLive – convenient shorthand
 */
export const MemoryStorage = MemoryStorageLive
