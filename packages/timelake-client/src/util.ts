/**
 * Helpers the reader and the writer share: table names, times, durations and URLs, as the engine parses them. Durations
 * are parsed by the WebAssembly module (`timelake-format`'s rules); table names are checked here too, where a handle is
 * made before the module is loaded, and a test keeps both in step.
 */

import { ensureWasm } from "./init.ts"
import { parseDuration } from "./wasm/timelake_wasm.js"

/** Namespace of table names given without one. */
export const DEFAULT_NAMESPACE = "default"

/** A point in time: Date, epoch milliseconds, or an ISO 8601 string (UTC unless it has an offset). */
export type TimeInput = Date | string | number

/** Duration units as the engine's `parse_duration` names them. */
type DurationUnit =
  | "s"
  | "sec"
  | "secs"
  | "second"
  | "seconds"
  | "m"
  | "min"
  | "mins"
  | "minute"
  | "minutes"
  | "h"
  | "hr"
  | "hrs"
  | "hour"
  | "hours"
  | "d"
  | "day"
  | "days"
  | "w"
  | "week"
  | "weeks"
  | "y"
  | "year"
  | "years"
/** A duration: `"30s"`, `"10m"`, `"1h"`, `"1d"`, `"1w"`, `"1y"`, spelled out (`"3 hours"`), or milliseconds. */
export type DurationInput = `${number}${DurationUnit}` | `${number} ${DurationUnit}` | number

const NAME_PART = /^[A-Za-z0-9_-]+$/

/**
 * Splits `[namespace.]table` as the engine's `TableName` does: both parts are `[A-Za-z0-9_-]+`, so they are safe as
 * path segments, and a name without a namespace is in `default`.
 */
export function parseTableName(table: string): { namespace: string; name: string } {
  const dot = table.indexOf(".")
  const [namespace, name] = dot < 0 ? [DEFAULT_NAMESPACE, table] : [table.slice(0, dot), table.slice(dot + 1)]
  if (!NAME_PART.test(namespace) || !NAME_PART.test(name)) {
    throw new Error(`table name ${JSON.stringify(table)} must be [namespace.]table using [A-Za-z0-9_-]`)
  }
  return { namespace, name }
}

/** The warehouse's table index, for stores that can't list (`timelake-format`'s `tables_index_uri`). */
export function tablesIndex(warehouse: string): string {
  return `${warehouse}/timelake-tables.json`
}

/** `s` without trailing slashes. Not `/\/+$/`: on a string with many slashes that regex backtracks in quadratic time. */
export function trimTrailingSlashes(s: string): string {
  let end = s.length
  while (end > 0 && s[end - 1] === "/") end--
  return s.slice(0, end)
}

/** A date, then optionally a time (seconds and fraction optional) and an offset. */
const ISO_TIME = /^(\d{4}-\d{2}-\d{2})(?:[T ](\d{2}:\d{2})(?::(\d{2})(?:\.(\d+))?)?(?:(Z)|([+-]\d{2}):?(\d{2}))?)?$/i

/**
 * Microseconds since the epoch. Strings must be ISO 8601; one without an offset is UTC, as in the engine's SQL, so a
 * query means the same in every time zone. A string keeps microseconds of its fraction; a number is milliseconds.
 */
export function timeMicros(t: TimeInput): number {
  let micros = NaN
  if (t instanceof Date) micros = t.getTime() * 1000
  else if (typeof t === "number") micros = t * 1000
  else {
    const m = ISO_TIME.exec(t.trim())
    if (m) {
      const [, date, hm = "00:00", sec = "00", fraction = "", , offsetHours, offsetMinutes] = m
      const offset = offsetHours === undefined ? "Z" : `${offsetHours}:${offsetMinutes ?? "00"}`
      const ms = Date.parse(`${date}T${hm}:${sec}${offset}`)
      micros = ms * 1000 + Number(fraction.slice(0, 6).padEnd(6, "0"))
    }
  }
  if (!Number.isFinite(micros)) {
    throw new Error(`invalid time: ${String(t)} (use a Date, epoch milliseconds, or an ISO 8601 string)`)
  }
  return Math.round(micros)
}

/**
 * Microseconds of a duration. Strings follow the engine's syntax (`parse_duration`): a whole number and a unit, `s`,
 * `m`, `h`, `d`, `w` or `y` (365 days), or spelled out (`"3 hours"`, `"1 day"`). A number is milliseconds.
 */
export function durationMicros(d: DurationInput): number {
  if (typeof d === "number") {
    if (!Number.isFinite(d)) throw new Error(`invalid duration: ${d}`)
    return Math.round(d * 1000)
  }
  ensureWasm()
  try {
    return parseDuration(d)
  } catch (e) {
    const why = e instanceof Error ? e.message : String(e)
    throw new Error(`invalid duration: ${JSON.stringify(d)}: ${why}`, { cause: e })
  }
}
