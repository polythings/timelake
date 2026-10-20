"use client"

import { CircleAlert } from "lucide-react"
import { useState } from "react"

import { DotGrid } from "@/components/dots"
import { EntryLabel } from "@/components/entry-label"
import { Segmented } from "@/components/segmented"
import { type Rank, type Ranking, entryAnchor, entryDescription, systemColor } from "@/lib/bench-systems"
import { formatBytes, formatCompact, formatNumber, formatTime, formatUsd } from "@/lib/format"
import { cn } from "@/lib/utils"

/** Columns of squares in the longest bar of a card. */
const SCALE = 40

/** A card's heading: a title, a control at its right such as a metric switch, and one muted note. */
export function PanelHeader({
  title,
  note,
  action,
}: {
  title: React.ReactNode
  note?: React.ReactNode
  action?: React.ReactNode
}) {
  return (
    <div className="flex flex-col gap-0.5">
      <div className="flex flex-wrap items-start justify-between gap-x-4 gap-y-2">
        <h3 className="text-lg font-normal">{title}</h3>
        {action}
      </div>
      {note && <p className="text-xs text-muted-foreground">{note}</p>}
    </div>
  )
}

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
  action?: React.ReactNode
  children: React.ReactNode
  className?: string
}) {
  return (
    <div className={cn("flex min-w-0 flex-col gap-4 rounded-lg border bg-card p-4 sm:p-6", className)}>
      <PanelHeader title={title} note={note} action={action} />
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
  // Bars are scaled to the largest value; where every value is 0, all bars are the shortest.
  const largest = Math.max(0, ...rows.map((r) => r.value))
  const top = right[0] ?? rows[0]
  const best = top?.value ?? 0
  const show = (r: Rank) => formatRankValue(r.value, ranking.unit)
  // A bar is two rows of squares, as long as its value against the largest.
  const columns = (value: number) => (largest > 0 ? Math.max(1, Math.round((value / largest) * SCALE)) : 1)
  const list = (rs: Rank[]) => (
    <ol className="flex flex-col gap-2.5">
      {rs.map((r) => {
        const ratio = ranking.higherIsBetter ? best / Math.max(r.value, 1e-9) : best > 0 ? r.value / best : 1
        return (
          <li
            key={r.key}
            className="grid grid-cols-[minmax(0,1fr)_5rem] items-center gap-x-3 gap-y-1 sm:grid-cols-[minmax(0,12rem)_5rem_minmax(0,1fr)]"
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
              className={cn(
                "text-right font-mono text-xs tabular-nums",
                r.timelake ? "text-foreground" : "text-muted-foreground",
              )}
            >
              {show(r)}
            </span>
            <span className="col-span-2 flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 sm:col-span-1">
              <DotGrid
                perRow={columns(r.value)}
                segments={[{ count: columns(r.value) * 2, color: systemColor(r.system) }]}
                label={`${r.label}: ${show(r)}`}
              />
              {r !== top ? (
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
    note: "what one query pays for object-storage requests at list prices, first run; machines and storage not included",
  },
  { key: "bytes", label: "Data", note: "bytes read from object storage for one query, first run" },
]

const UNITS: Record<Exclude<Metric, "time">, Ranking["unit"]> = {
  requests: "requests",
  usd: "usd",
  bytes: "bytes",
}

/** A value in a ranking's unit: "48 ms", "1.2M/s", "12 requests", "$0.40", "1.3 MB". */
export function formatRankValue(v: number, unit: Ranking["unit"]): string {
  switch (unit) {
    case "rows/s":
      return `${formatCompact(v)}/s`
    case "requests":
      return formatNumber(Math.round(v))
    case "usd":
      return formatUsd(v)
    case "bytes":
      return formatBytes(Math.round(v))
    case "milliseconds":
      return formatTime(v / 1000)
    default:
      return formatTime(v)
  }
}

/** The ranking by `metric`: its rows that measured it, fewest first. */
export function byMetric(ranking: Ranking, metric: Metric): Ranking {
  if (metric === "time") return ranking
  const of = (r: Rank) => (metric === "usd" ? (r.usd ?? NaN) : metric === "requests" ? r.requests : r.bytes)
  const rows = ranking.rows
    .filter((r) => Number.isFinite(of(r)))
    .map((r) => ({ ...r, value: of(r) ?? 0 }))
    .sort((a, b) => a.value - b.value)
  const note = METRICS.find((m) => m.key === metric)?.note ?? ""
  return { ...ranking, rows, unit: UNITS[metric], higherIsBetter: false, note }
}

/** The metrics a ranking can switch between: time always, the others where the run counted them (object storage). */
export function metricOptions(ranking: Ranking): { value: Metric; label: string }[] {
  const timed = ranking.unit === "seconds"
  return METRICS.filter(
    (m) => m.key === "time" || (timed && ranking.rows.some((r) => r[m.key as Exclude<Metric, "time">] !== undefined)),
  ).map((m) => ({ value: m.key, label: m.label }))
}

/** A switch between a ranking's metrics, or nothing where time is the only one. */
export function MetricSwitch({
  options,
  value,
  onChange,
}: {
  options: { value: Metric; label: string }[]
  value: Metric
  onChange: (m: Metric) => void
}) {
  return options.length > 1 ? (
    <Segmented options={options} value={value} onChange={onChange} label="What to rank by" />
  ) : null
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
  const [metric, setMetric] = useState<Metric>("time")
  const options = metricOptions(ranking)
  const shown = byMetric(ranking, options.some((o) => o.value === metric) ? metric : "time")
  return (
    <Panel
      title={ranking.title}
      note={`${shown.note}; ${shown.higherIsBetter ? "higher" : "lower"} is better`}
      action={<MetricSwitch options={options} value={metric} onChange={setMetric} />}
    >
      <RankingBars ranking={shown} perSystem={perSystem} queries={ranking.queries} learnMore={learnMore} />
    </Panel>
  )
}
