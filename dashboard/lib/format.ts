const BYTE_UNITS = ["KB", "MB", "GB", "TB"]

/** 1500 → "1.5 KB": decimal units (1 MB = 1,000,000 bytes), one decimal under 10. */
export function formatBytes(n: number): string {
  if (n < 1000) return `${n} B`
  let v = n / 1000
  let i = 0
  // Up a unit where the value would round to 1000 in this one.
  while (v >= 999.5 && i < BYTE_UNITS.length - 1) {
    v /= 1000
    i++
  }
  return `${v.toFixed(v < 9.95 ? 1 : 0)} ${BYTE_UNITS[i]}`
}

/** 1234567 → "1,234,567"; with `digits`, exactly that many decimals: (1234.5, 2) → "1,234.50". */
export function formatNumber(n: number, digits?: number): string {
  const decimals = digits === undefined ? {} : { minimumFractionDigits: digits, maximumFractionDigits: digits }
  return new Intl.NumberFormat("en-US", decimals).format(n)
}

/** Dollars with cents from a dollar up ("$2.40"), below that to two significant digits ("$0.023", "$0.000021"). */
export function formatUsd(n: number): string {
  if (n === 0) return "$0"
  if (n >= 1) return `$${n.toFixed(2)}`
  return `$${n.toLocaleString("en-US", { minimumSignificantDigits: 2, maximumSignificantDigits: 2 })}`
}

/** 1234567 → "1.2M". */
export function formatCompact(n: number): string {
  return new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(n)
}

/** Dollars with `digits` decimals: (0.4, 2) → "$0.40"; tiny amounts take more, (0.00404, 4) → "$0.0040". */
export function formatUsd(x: number, digits = 2): string {
  return `$${x.toFixed(digits)}`
}

/**
 * Seconds as milliseconds below a second ("40 ms", "0.4 ms"), else seconds ("2.31 s", "17.8 s", "325 s"): each value
 * in the unit that reads best, so "40 ms" and "30 ms" never show as "0.04 s" and "0.03 s". The unit is picked after
 * rounding, so 0.9996 s is "1.00 s", not "1000 ms".
 */
export function formatTime(s: number): string {
  const ms = s * 1000
  if (ms < 9.95) return `${ms.toFixed(1).replace(/\.0$/, "")} ms`
  if (ms < 999.5) return `${Math.round(ms)} ms`
  if (s < 9.995) return `${s.toFixed(2)} s`
  if (s < 99.95) return `${s.toFixed(1)} s`
  return `${Math.round(s)} s`
}

/** A step's length: "4.2 s" under 100 s, "325 s" under 10 minutes, then minutes ("14 min"). */
export function formatDuration(s: number): string {
  if (s < 99.95) return `${s.toFixed(1)} s`
  if (s < 599.5) return `${Math.round(s)} s`
  return `${Math.round(s / 60)} min`
}

/** Milliseconds, whole: 245.3 → "245 ms". */
export function formatMs(ms: number): string {
  return `${Math.round(ms)} ms`
}

/** A value through `format`, or a dash where there is none. */
export function orDash<T>(value: T | null | undefined, format: (v: T) => string): string {
  return value == null ? "—" : format(value)
}
