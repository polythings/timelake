const BYTE_UNITS = ["KB", "MB", "GB", "TB"]

/** 1536 → "1.5 KB" (binary units, one decimal under 10). */
export function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`
  let v = n / 1024
  let i = 0
  while (v >= 1024 && i < BYTE_UNITS.length - 1) {
    v /= 1024
    i++
  }
  return `${v.toFixed(v < 10 ? 1 : 0)} ${BYTE_UNITS[i]}`
}

/** 1234567 → "1,234,567". */
export function formatNumber(n: number): string {
  return new Intl.NumberFormat("en-US").format(n)
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

/**
 * Seconds as milliseconds below a second ("40 ms", "0.4 ms"), else seconds ("2.31 s", "17.8 s", "325 s"): each value
 * in the unit that reads best, so "40 ms" and "30 ms" never show as "0.04 s" and "0.03 s".
 */
export function formatTime(s: number): string {
  if (s < 1) {
    const ms = s * 1000
    return `${ms < 10 ? ms.toFixed(1).replace(/\.0$/, "") : Math.round(ms)} ms`
  }
  return `${s.toFixed(s < 10 ? 2 : s < 100 ? 1 : 0)} s`
}
