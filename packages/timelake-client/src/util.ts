/** Helpers the reader and the writer share: table names, times, durations and URLs, as the engine parses them. */

/** Namespace of table names given without one. */
export const DEFAULT_NAMESPACE = "default"

/** A point in time: Date, epoch milliseconds, or an ISO 8601 string (UTC unless it has an offset). */
export type TimeInput = Date | string | number

/** Seconds of each duration unit, as the engine's `parse_duration` names them. */
const UNIT_SECONDS = {
  ...units(1, "s", "sec", "secs", "second", "seconds"),
  ...units(60, "m", "min", "mins", "minute", "minutes"),
  ...units(3600, "h", "hr", "hrs", "hour", "hours"),
  ...units(86_400, "d", "day", "days"),
  ...units(7 * 86_400, "w", "week", "weeks"),
  ...units(365 * 86_400, "y", "year", "years"),
}

function units<const N extends string>(seconds: number, ...names: N[]): Record<N, number> {
  return Object.fromEntries(names.map((n) => [n, seconds])) as Record<N, number>
}

type DurationUnit = keyof typeof UNIT_SECONDS
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
  const s = d.trim()
  const m = /^(\d+)(.*)$/.exec(s)
  const unit = m?.[2]?.trim().toLowerCase()
  const seconds =
    unit !== undefined && Object.hasOwn(UNIT_SECONDS, unit) ? UNIT_SECONDS[unit as DurationUnit] : undefined
  if (!m?.[1] || seconds === undefined) {
    throw new Error(`invalid duration: ${JSON.stringify(d)} (use a whole number and s, m, h, d, w or y, e.g. 10m)`)
  }
  const micros = Number(m[1]) * seconds * 1e6
  if (!Number.isSafeInteger(micros)) throw new Error(`duration ${JSON.stringify(d)} is too long`)
  return micros
}
