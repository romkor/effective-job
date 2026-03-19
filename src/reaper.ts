import { Cause, Effect, Fiber, Queue as EQueue, Ref, Scope, Stream } from "effect"
import { randomUUID } from "node:crypto"
import { createJsonSerde } from "./serde.ts"
import { Storage } from "./storage/service.ts"
import type { QueueMessage, Serde } from "./types.ts"
import { parseState } from "./utils/state.ts"

export interface LeaderElectionConfig {
  readonly enabled: boolean
  readonly lockTTL?: number
  readonly renewalInterval?: number
  readonly acquireRetryInterval?: number
}

export interface ReaperConfig<TPayload = unknown> {
  readonly payloadSerde?: Serde<TPayload>
  readonly visibilityTimeout?: number
  readonly leaderElection?: LeaderElectionConfig
}

export type ReaperEvent =
  | { readonly _tag: "stalled"; readonly id: string }
  | { readonly _tag: "leadershipAcquired" }
  | { readonly _tag: "leadershipLost" }
  | { readonly _tag: "error"; readonly error: Error }

const LOCK_KEY = "reaper:lock"
const DEFAULT_LOCK_TTL = 30_000
const DEFAULT_RENEWAL_INTERVAL = 10_000
const DEFAULT_ACQUIRE_RETRY_INTERVAL = 5_000

/**
 * Create a Reaper that monitors for stalled jobs and requeues them.
 *
 * Returns an object with:
 * - `start`: Effect that begins monitoring
 * - `stop`: Effect that stops monitoring
 * - `events`: Stream of reaper events
 *
 * The reaper uses event-based monitoring: it subscribes to job state changes
 * and sets per-job timers. An initial scan at startup catches any jobs that
 * were processing before the reaper started.
 */
export const makeReaper = <TPayload = unknown>(
  config: ReaperConfig<TPayload> = {}
): Effect.Effect<{
  readonly start: Effect.Effect<void>
  readonly stop: Effect.Effect<void>
  readonly events: Stream.Stream<ReaperEvent>
  readonly reaperId: string
}, never, Storage | Scope.Scope> =>
  Effect.gen(function* () {
    const storage = yield* Storage
    const payloadSerde = config.payloadSerde ?? createJsonSerde<TPayload>()
    const visibilityTimeout = config.visibilityTimeout ?? 30_000
    const leaderElection = config.leaderElection ?? { enabled: false }
    const reaperId = randomUUID()

    const eventQueue = yield* EQueue.unbounded<ReaperEvent>()
    const emitEvent = (e: ReaperEvent): Effect.Effect<void> => EQueue.offer(eventQueue, e).pipe(Effect.ignore)

    /** Catch any error/defect from an Effect and emit it as an error event */
    const catchAndEmitError = <A>(effect: Effect.Effect<A>): Effect.Effect<A | void> =>
      effect.pipe(
        Effect.catchAllCause((cause) => {
          const raw = Cause.squash(cause)
          const err = raw instanceof Error ? raw : new Error(String(raw))
          return emitEvent({ _tag: "error", error: err })
        })
      )

    // ── Mutable state ─────────────────────────────────────────────
    const runningRef = yield* Ref.make(false)
    const isLeaderRef = yield* Ref.make(false)
    const unsubscribeRef = yield* Ref.make<Effect.Effect<void> | null>(null)
    const timerFibersRef = yield* Ref.make(new Map<string, Fiber.RuntimeFiber<void>>())
    const leadershipFiberRef = yield* Ref.make<Fiber.RuntimeFiber<void> | null>(null)

    // ── Timer helpers ─────────────────────────────────────────────

    const cancelTimer = (id: string): Effect.Effect<void> =>
      Effect.gen(function* () {
        const map = yield* Ref.get(timerFibersRef)
        const fiber = map.get(id)
        if (fiber) {
          yield* Fiber.interrupt(fiber)
          yield* Ref.update(timerFibersRef, (m) => { m.delete(id); return m })
        }
      })

    const startTimer = (id: string, delayMs: number): Effect.Effect<void> =>
      Effect.gen(function* () {
        yield* cancelTimer(id)
        const fiber = yield* Effect.forkDaemon(
          Effect.sleep(`${delayMs} millis`).pipe(
            Effect.flatMap(() =>
              Effect.gen(function* () {
                yield* Ref.update(timerFibersRef, (m) => { m.delete(id); return m })
                yield* checkJob(id)
              })
            )
          )
        )
        yield* Ref.update(timerFibersRef, (m) => { m.set(id, fiber); return m })
      })

    // ── Job checking ──────────────────────────────────────────────

    const recoverStalledJob = (id: string, workerId: string): Effect.Effect<void> =>
      Effect.gen(function* () {
        const processingJobs = yield* storage.getProcessingJobs(workerId)

        let jobMessage: Buffer | null = null
        for (const msg of processingJobs) {
          try {
            const parsed = payloadSerde.deserialize(msg as Buffer) as unknown as QueueMessage<TPayload>
            if (parsed.id === id) {
              jobMessage = msg as Buffer
              break
            }
          } catch {
            // ignore deserialization errors
          }
        }

        if (!jobMessage) return

        yield* storage.requeue(id, jobMessage, workerId)

        const parsed = payloadSerde.deserialize(jobMessage) as unknown as QueueMessage<TPayload>
        yield* storage.setJobState(id, `failing:${Date.now()}:${parsed.attempts + 1}`)
        yield* emitEvent({ _tag: "stalled", id })
      })

    const checkJob = (id: string): Effect.Effect<void> =>
      Effect.gen(function* () {
        const running = yield* Ref.get(runningRef)
        if (!running) return

        const isLeader = yield* Ref.get(isLeaderRef)
        if (leaderElection.enabled && !isLeader) return

        const state = yield* storage.getJobState(id)
        if (state === null) return

        const { status, timestamp, workerId } = parseState(state)
        if (status !== "processing") return

        const elapsed = Date.now() - timestamp
        if (elapsed < visibilityTimeout) {
          // Not yet stalled – restart timer for remaining time
          yield* startTimer(id, visibilityTimeout - elapsed)
          return
        }

        if (workerId) {
          yield* recoverStalledJob(id, workerId)
        }
      }).pipe(catchAndEmitError)

    const handleEvent = (id: string, event: string): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (event === "processing") {
          yield* startTimer(id, visibilityTimeout)
        } else if (event === "completed" || event === "failed" || event === "cancelled") {
          yield* cancelTimer(id)
        }
      })

    const checkStalledJobs = (): Effect.Effect<void> =>
      Effect.gen(function* () {
        const running = yield* Ref.get(runningRef)
        if (!running) return

        const isLeader = yield* Ref.get(isLeaderRef)
        if (leaderElection.enabled && !isLeader) return

        const workers = yield* storage.getWorkers()
        for (const workerId of workers) {
          const jobs = yield* storage.getProcessingJobs(workerId)
          for (const msg of jobs) {
            try {
              const parsed = payloadSerde.deserialize(msg as Buffer) as unknown as QueueMessage<TPayload>
              const state = yield* storage.getJobState(parsed.id)
              if (state === null) continue

              const { status, timestamp } = parseState(state)
              if (status !== "processing") continue

              const elapsed = Date.now() - timestamp
              const timers = yield* Ref.get(timerFibersRef)

              if (elapsed >= visibilityTimeout) {
                yield* recoverStalledJob(parsed.id, workerId)
              } else if (!timers.has(parsed.id)) {
                yield* startTimer(parsed.id, visibilityTimeout - elapsed)
              }
            } catch {
              // ignore deserialization errors
            }
          }
        }
      })

    // ── Active / inactive ─────────────────────────────────────────

    const becomeActive = (): Effect.Effect<void> =>
      Effect.gen(function* () {
        const unsubscribe = yield* storage.subscribeToEvents((id, event) => {
          Effect.runFork(handleEvent(id, event))
        })
        yield* Ref.set(unsubscribeRef, unsubscribe)
        yield* checkStalledJobs()
      })

    const becomeInactive = (): Effect.Effect<void> =>
      Effect.gen(function* () {
        // Cancel all timers
        const timers = yield* Ref.get(timerFibersRef)
        for (const fiber of timers.values()) {
          yield* Fiber.interrupt(fiber)
        }
        yield* Ref.set(timerFibersRef, new Map())

        // Unsubscribe from events
        const unsubscribe = yield* Ref.get(unsubscribeRef)
        if (unsubscribe !== null) {
          yield* unsubscribe
          yield* Ref.set(unsubscribeRef, null)
        }
      })

    // ── Leader election ───────────────────────────────────────────

    const tryAcquireLock = (ttlMs: number): Effect.Effect<boolean> =>
      Effect.gen(function* () {
        if (!storage.acquireLeaderLock) {
          yield* emitEvent({ _tag: "error", error: new Error("Storage does not support leader election") })
          return false
        }
        return yield* storage.acquireLeaderLock(LOCK_KEY, reaperId, ttlMs)
      })

    const tryRenewLock = (ttlMs: number): Effect.Effect<boolean> =>
      storage.renewLeaderLock
        ? storage.renewLeaderLock(LOCK_KEY, reaperId, ttlMs)
        : Effect.succeed(false)

    const releaseLeadership = (): Effect.Effect<void> =>
      storage.releaseLeaderLock
        ? storage.releaseLeaderLock(LOCK_KEY, reaperId).pipe(Effect.ignore)
        : Effect.void

    const transitionToLeader = (): Effect.Effect<void> =>
      Effect.gen(function* () {
        yield* Ref.set(isLeaderRef, true)
        yield* becomeActive()
        yield* emitEvent({ _tag: "leadershipAcquired" })
      })

    const transitionToFollower = (): Effect.Effect<void> =>
      Effect.gen(function* () {
        yield* Ref.set(isLeaderRef, false)
        yield* becomeInactive()
        yield* emitEvent({ _tag: "leadershipLost" })
      })

    const startLeadershipLoop = (): Effect.Effect<void> =>
      Effect.gen(function* () {
        const lockTTL = leaderElection.lockTTL ?? DEFAULT_LOCK_TTL
        const renewalInterval = leaderElection.renewalInterval ?? DEFAULT_RENEWAL_INTERVAL
        const acquireRetryInterval = leaderElection.acquireRetryInterval ?? DEFAULT_ACQUIRE_RETRY_INTERVAL

        const acquired = yield* tryAcquireLock(lockTTL)
        if (acquired) {
          yield* transitionToLeader()
        }

        const leadershipLoop: Effect.Effect<void> = Effect.forever(
          Effect.gen(function* () {
            const isLeader = yield* Ref.get(isLeaderRef)
            const interval = isLeader ? renewalInterval : acquireRetryInterval
            yield* Effect.sleep(`${interval} millis`)

            const running = yield* Ref.get(runningRef)
            if (!running) return

            if (isLeader) {
              const renewed = yield* tryRenewLock(lockTTL)
              if (!renewed) yield* transitionToFollower()
            } else {
              const got = yield* tryAcquireLock(lockTTL)
              if (got) yield* transitionToLeader()
            }
          }).pipe(catchAndEmitError)
        )

        const fiber = yield* Effect.forkDaemon(leadershipLoop)
        yield* Ref.set(leadershipFiberRef, fiber)
      })

    // ── Public interface ──────────────────────────────────────────

    const start: Effect.Effect<void> = Effect.gen(function* () {
      const running = yield* Ref.get(runningRef)
      if (running) return
      yield* Ref.set(runningRef, true)

      if (leaderElection.enabled) {
        yield* startLeadershipLoop()
      } else {
        yield* Ref.set(isLeaderRef, true)
        yield* becomeActive()
      }
    })

    const stop: Effect.Effect<void> = Effect.gen(function* () {
      const running = yield* Ref.get(runningRef)
      if (!running) return
      yield* Ref.set(runningRef, false)

      // Stop leadership loop
      const leaderFiber = yield* Ref.get(leadershipFiberRef)
      if (leaderFiber) {
        yield* Fiber.interrupt(leaderFiber)
        yield* Ref.set(leadershipFiberRef, null)
      }

      // Release lock if leader
      const isLeader = yield* Ref.get(isLeaderRef)
      if (isLeader && leaderElection.enabled) {
        yield* releaseLeadership()
        yield* Ref.set(isLeaderRef, false)
      }

      yield* becomeInactive()
    })

    const events: Stream.Stream<ReaperEvent> = Stream.fromQueue(eventQueue, { shutdown: false })

    return { start, stop, events, reaperId }
  })
