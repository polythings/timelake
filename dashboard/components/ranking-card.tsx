"use client"

import { CircleAlert } from "lucide-react"
import { useState } from "react"

import { DotGrid } from "@/components/dots"
import { EntryLabel } from "@/components/entry-label"
import { type Rank, type Ranking, SYSTEM_COLORS, entryAnchor, entryDescription } from "@/lib/bench-systems"
import { formatBytes, formatCompact, formatTime } from "@/lib/format"
import { cn } from "@/lib/utils"

/** Columns of squares in the longest bar of a card. */
const SCALE = 40
/** Results within 5% of the fastest get no multiplier. */
const SAME_AS_FASTEST = 1.05

/** A card in the benchmark pages' style: a title, one muted note, then its content. */
export function Panel({
  title,
  note,
  action,
  children,
  className,
}: {
  title: React.ReactNode
  note?: React.ReactNode
  /** A control at the title's right, such as a metric switch. */
  action?: React.ReactNode
  children: React.ReactNode
  className?: string
}) {
  return (
    <div className={`flex min-w-0 flex-col gap-4 rounded-lg border bg-card p-6 ${className ?? ""}`}>
      <div className="flex flex-col gap-0.5">
        <div className="flex flex-wrap items-start justify-between gap-x-4 gap-y-2">
          <h3 className="text-lg font-normal">{title}</h3>
          {action}
        </div>
        {note && <p className="text-xs text-muted-foreground">{note}</p>}
      </div>
      {children}
    </div>
  )
}

/**
 * A ranking's bars: each row's value and a bar of squares in its system's color, best first, with how many times
 * worse than the best it is. Rows that gave wrong answers are listed apart, below, on the same scale: their times
 * aren't comparable with the right answers' times.
 */
export function RankingBars({
  ranking,
  perSystem = true,
  queries = 1,
  learnMore,
}: {
  ranking: Ranking
  perSystem?: boolean
  /** How many queries a row's value is over, for "2 of 5 wrong". */
  queries?: number
  /** The benchmark page whose list of entries an entry's "learn more" opens ("" for this page). */
  learnMore?: string
}) {
  // One row per system in each group: its best engine there (rows come best first), so a system's reader with right
  // answers isn't hidden behind a faster one with wrong answers. Timelake's own reader is always shown.
  const firstPerSystem = (rs: Rank[]) =>
    perSystem ? rs.filter((r, i, all) => r.timelake || all.findIndex((x) => x.system === r.system) === i) : rs
  const right = firstPerSystem(ranking.rows.filter((r) => !r.wrong))
  const wrong = firstPerSystem(ranking.rows.filter((r) => r.wrong))
  const rows = [...right, ...wrong]
  const largest = Math.max(...rows.map((r) => r.value))
  const best = (right[0] ?? rows[0])?.value ?? 0
  const show = (r: Rank) => formatValue(r.value, ranking.unit)
  // A bar is two rows of squares, as long as its value against the largest.
  const columns = (value: number) => Math.max(1, Math.round((value / largest) * SCALE))
  const list = (rs: Rank[]) => (
    <ol className="flex flex-col gap-2.5">
      {rs.map((r) => {
        const ratio = ranking.higherIsBetter ? best / Math.max(r.value, 1e-9) : best > 0 ? r.value / best : 1
        return (
          <li
            key={r.key}
            className="grid grid-cols-[minmax(0,1fr)_4.5rem] items-center gap-x-3 gap-y-1 sm:grid-cols-[minmax(0,12rem)_4.5rem_minmax(0,1fr)]"
          >
            <EntryLabel
              label={r.label}
              description={entryDescription(r.key)}
              details={[
                ...(r.repeat === undefined ? [] : [`best repeat ${formatTime(r.repeat)}`]),
                ...(r.requests === undefined ? [] : [`${Math.round(r.requests)} requests`]),
                ...(r.bytes === undefined ? [] : [`${formatBytes(Math.round(r.bytes))} read`]),
              ]}
              href={learnMore === undefined ? undefined : `${learnMore}#${entryAnchor(r.key)}`}
              className={r.timelake ? "text-sm" : "text-sm text-muted-foreground"}
            />
            <span
              className={`text-right font-mono text-xs tabular-nums ${r.timelake ? "text-foreground" : "text-muted-foreground"}`}
            >
              {show(r)}
            </span>
            <span className="col-span-2 flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 sm:col-span-1">
              <DotGrid
                perRow={columns(r.value)}
                segments={[{ count: columns(r.value) * 2, color: SYSTEM_COLORS[r.system] ?? "var(--chart-2)" }]}
                label={`${r.label}: ${show(r)}`}
              />
              {ratio > SAME_AS_FASTEST ? (
                <span className="font-mono text-[11px] text-muted-foreground tabular-nums">
                  {ratio < 10 ? ratio.toFixed(1) : Math.round(ratio)}×
                </span>
              ) : null}
              {(r.wrong ?? 0) > 0 && queries > 1 && (
                <span className="font-mono text-[11px] text-muted-foreground tabular-nums">
                  {r.wrong} of {queries} wrong
                </span>
              )}
            </span>
          </li>
        )
      })}
    </ol>
  )
  return (
    <div className="flex flex-col gap-4">
      {right.length > 0 && list(right)}
      {wrong.length > 0 && (
        <div className="flex flex-col gap-2.5 border-t pt-4">
          <p className="flex items-start gap-1.5 text-xs text-muted-foreground">
            <CircleAlert className="mt-px size-3.5 shrink-0" aria-hidden />
            {queries > 1
              ? "Wrong answers to some queries: their results don't match the data with each row counted once, so they aren't ranked with the ones above."
              : "Wrong answer: its result doesn't match the data with each row counted once, so it isn't ranked with the ones above."}
          </p>
          {list(wrong)}
        </div>
      )}
    </div>
  )
}

/** What a ranking of query times can show instead: requests, their price, bytes read (object storage only). */
export type Metric = "time" | "requests" | "usd" | "bytes"

const METRICS: { key: Metric; label: string; note: string }[] = [
  { key: "time", label: "Time", note: "" },
  {
    key: "requests",
    label: "Requests",
    note: "requests to object storage for one query, first run; database servers read their own disk and aren't counted",
  },
  {
    key: "usd",
    label: "Cost",
    note: "what a million of these queries pay for object-storage requests at list prices; machines and storage not included",
  },
  { key: "bytes", label: "Data", note: "bytes read from object storage for one query, first run" },
]

const UNITS: Record<Exclude<Metric, "time">, Ranking["unit"]> = {
  requests: "requests",
  usd: "usd per million",
  bytes: "bytes",
}

/** A value in a ranking's unit: "48 ms", "1.2M/s", "12 requests", "$0.40", "1.3 MB". */
function formatValue(v: number, unit: Ranking["unit"]): string {
  switch (unit) {
    case "rows/s":
      return `${formatCompact(v)}/s`
    case "requests":
      return Math.round(v).toLocaleString("en-US")
    case "usd per million":
      return `$${v < 10 ? v.toFixed(2) : Math.round(v)}`
    case "bytes":
      return formatBytes(Math.round(v))
    case "milliseconds":
      return formatTime(v / 1000)
    default:
      return formatTime(v)
  }
}

/** The ranking by `metric`: its rows that measured it, fewest first. */
function byMetric(ranking: Ranking, metric: Metric): Ranking {
  if (metric === "time") return ranking
  const of = (r: Rank) => (metric === "usd" ? (r.usd ?? NaN) * 1e6 : metric === "requests" ? r.requests : r.bytes)
  const rows = ranking.rows
    .filter((r) => Number.isFinite(of(r)))
    .map((r) => ({ ...r, value: of(r) ?? 0 }))
    .sort((a, b) => a.value - b.value)
  const note = METRICS.find((m) => m.key === metric)?.note ?? ""
  return { ...ranking, rows, unit: UNITS[metric], higherIsBetter: false, note }
}

/**
 * The metrics a query ranking can switch between, and the one shown: time always, the others where the run counted
 * them (object storage).
 */
export function useMetric(ranking: Ranking) {
  const [metric, setMetric] = useState<Metric>("time")
  const timed = ranking.unit === "seconds"
  const options = METRICS.filter(
    (m) => m.key === "time" || (timed && ranking.rows.some((r) => r[m.key as Exclude<Metric, "time">] !== undefined)),
  )
  const shown = byMetric(ranking, options.some((m) => m.key === metric) ? metric : "time")
  const action =
    options.length > 1 ? (
      <div className="flex rounded-md border p-0.5 text-xs" role="group" aria-label="What to rank by">
        {options.map((m) => (
          <button
            key={m.key}
            type="button"
            aria-pressed={metric === m.key}
            onClick={() => setMetric(m.key)}
            className={cn(
              "rounded px-2 py-0.5 text-muted-foreground transition-colors hover:text-foreground",
              metric === m.key && "bg-muted text-foreground",
            )}
          >
            {m.label}
          </button>
        ))}
      </div>
    ) : null
  return { shown, action }
}

/** One ranking as a card, by time or, where counted, by requests, their cost or bytes read. */
export function RankingCard({
  ranking,
  perSystem = true,
  learnMore,
}: {
  ranking: Ranking
  perSystem?: boolean
  learnMore?: string
}) {
  const { shown, action } = useMetric(ranking)
  return (
    <Panel
      title={ranking.title}
      note={`${shown.note}; ${shown.higherIsBetter ? "higher" : "lower"} is better`}
      action={action}
    >
      <RankingBars ranking={shown} perSystem={perSystem} queries={ranking.queries} learnMore={learnMore} />
    </Panel>
  )
}
