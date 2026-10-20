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

/** Epoch milliseconds as "2026-01-03 12:00:00" in UTC, or to the minute. */
export function formatUtc(ms: number, precision: "seconds" | "minutes" = "seconds"): string {
  return new Date(ms)
    .toISOString()
    .replace("T", " ")
    .slice(0, precision === "seconds" ? 19 : 16)
}

/** How long before `now` a time was, e.g. "5m ago"; "never" for null. */
export function formatAgo(ms: number | null, now: number): string {
  if (ms == null) return "never"
  const s = Math.max(0, Math.round((now - ms) / 1000))
  if (s < 60) return `${s}s ago`
  const m = Math.round(s / 60)
  if (m < 60) return `${m}m ago`
  const h = Math.round(m / 60)
  if (h < 48) return `${h}h ago`
  return `${Math.round(h / 24)}d ago`
}

/** A query result cell: grouped integers, up to 4 decimals, "—" for null. */
export function formatValue(v: unknown): string {
  if (v === null || v === undefined) return "—"
  if (typeof v === "number") return Number.isInteger(v) ? formatNumber(v) : v.toFixed(4).replace(/\.?0+$/, "")
  return String(v)
}

export function tableHref(name: string): string {
  return `/explore/tables/${encodeURIComponent(name)}`
}
