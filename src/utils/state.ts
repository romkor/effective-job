import type { MessageState } from "../types.ts"

export interface ParsedState {
  readonly status: MessageState
  readonly timestamp: number
  readonly workerId?: string
  readonly attempts?: number
}

/**
 * Parse job state string into components.
 * State format: "status:timestamp" | "status:timestamp:workerId" | "failing:timestamp:attempts"
 */
export function parseState(state: string): ParsedState {
  const parts = state.split(":")
  const status = parts[0] as MessageState
  const timestamp = parseInt(parts[1] ?? "0", 10)

  if (status === "failing") {
    return {
      status,
      timestamp,
      attempts: parseInt(parts[2] ?? "0", 10)
    }
  }

  return {
    status,
    timestamp,
    workerId: parts[2] as string | undefined
  }
}
