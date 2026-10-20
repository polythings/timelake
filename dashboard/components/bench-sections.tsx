"use client"

import { ChevronDown, CircleAlert } from "lucide-react"
import { Fragment, useId, useState } from "react"

import { BenchHistory, type HistoryLine } from "@/components/bench-history"
import {
  type Metric,
  MetricSwitch,
  PanelHeader,
  RankingBars,
  byMetric,
  formatRankValue,
  metricOptions,
} from "@/components/ranking-card"
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import { useMediaQuery } from "@/hooks/use-mobile"
import QUERIES from "@/lib/bench-queries.json"
import { type Ranking, SYSTEM_COLORS, isOwnReader } from "@/lib/bench-systems"
import type { HistoryPoint, Section, SectionHistory } from "@/lib/benchmarks"
import { cn } from "@/lib/utils"

/** Each suite's queries as the query files describe them (bench/queries; timebench queries --site). */
const DESCRIBED = QUERIES as Record<string, { name: string; description: string; sql: string; tuned?: string }[]>

/** Where the results move beside the list (Tailwind's lg). */
const WIDE = "(min-width: 64rem)"

/** The list's comparison column, by metric. */
const VERSUS: Record<Metric, [header: string, best: string]> = {
  time: ["vs fastest", "Fastest"],
  requests: ["vs fewest", "Fewest"],
  usd: ["vs cheapest", "Cheapest"],
  bytes: ["vs least", "Least"],
}

/** One row of a section's list: all its queries together, or one query. */
type Entry = {
  id: string
  title: string
  description?: string
  sql?: string
  /** Timelake's version of the query in a tuned run, where it has one (D23). */
  tuned?: string
  ranking: Ranking
  failed: string[]
  points: HistoryPoint[]
}

/**
 * One suite at one checkpoint, as a list and the selected row's results: all queries together first, then each query
 * with Timelake's result and how it compares with the best right answer. On wide screens the results sit to the right
 * of the list; on narrow ones they open under the selected row. One switch ranks the list and the results by time or,
 * where counted, by requests, their cost, or bytes read.
 */
export function SectionResults({ section, history }: { section: Section; history: SectionHistory }) {
  const described = new Map((DESCRIBED[section.suite] ?? []).map((q) => [q.name, q]))
  const asked = section.queries.length
  const common = section.ranking.queries ?? asked
  const entries: Entry[] = [
    {
      id: "",
      title: "All queries",
      description:
        common === asked
          ? `The geometric mean over the ${asked} queries.`
          : `The geometric mean over the ${common} of ${asked} queries every reader answered.`,
      ranking: section.ranking,
      failed: [],
      points: history.all,
    },
    ...section.queries.map((q) => ({
      id: q.query,
      title: q.query,
      description: described.get(q.query)?.description,
      sql: described.get(q.query)?.sql,
      tuned: described.get(q.query)?.tuned,
      ranking: q.ranking,
      failed: q.failed,
      points: history.queries[q.query] ?? [],
    })),
  ]
  const wide = useMediaQuery(WIDE)
  // Wide screens always show one row's results beside the list; narrow ones open and close them under the row.
  const [selected, setSelected] = useState<string | null>("")
  const select = (id: string) => setSelected(id === selected && !wide ? null : id)
  const options = metricOptions(section.ranking)
  const [chosen, setMetric] = useState<Metric>("time")
  const metric = options.some((o) => o.value === chosen) ? chosen : "time"
  const panel = useId()
  const columns = "grid-cols-[minmax(0,1fr)_4rem_3.5rem_1rem] lg:grid-cols-[minmax(0,1fr)_4rem_3.5rem]"
  return (
    <div
      className="grid gap-x-6 rounded-lg border bg-card p-4 sm:p-6 lg:grid-cols-[minmax(0,5fr)_minmax(0,7fr)] lg:[grid-template-rows:repeat(var(--rows),auto)_1fr]"
      style={{ "--rows": entries.length + 1 } as React.CSSProperties}
    >
      <div className="flex flex-col gap-3 pb-2 lg:col-start-1">
        {options.length > 1 && (
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
            Rank by <MetricSwitch options={options} value={metric} onChange={setMetric} />
          </div>
        )}
        <div className={cn("grid gap-x-3 px-2 text-[11px] text-muted-foreground", columns)}>
          <span>Query</span>
          <span className="text-right">Timelake</span>
          <span className="text-right">{VERSUS[metric][0]}</span>
        </div>
      </div>
      {entries.map((e) => {
        const open = e.id === selected
        return (
          <Fragment key={e.id}>
            <Row
              entry={e}
              metric={metric}
              columns={columns}
              open={open}
              wide={wide}
              controls={panel}
              onSelect={() => select(e.id)}
            />
            {open && (
              <div
                id={panel}
                role="region"
                aria-label={e.title}
                className="my-2 rounded-md border p-4 lg:sticky lg:top-20 lg:col-start-2 lg:row-[1/-1] lg:my-0 lg:self-start lg:rounded-none lg:border-0 lg:border-l lg:py-0 lg:pr-0 lg:pl-6"
              >
                <Details key={e.id} entry={e} metric={metric} />
              </div>
            )}
          </Fragment>
        )
      })}
    </div>
  )
}

/** A list row: the query and its description, Timelake's result, and how it compares with the best right answer. */
function Row({
  entry,
  metric,
  columns,
  open,
  wide,
  controls,
  onSelect,
}: {
  entry: Entry
  metric: Metric
  columns: string
  open: boolean
  wide: boolean
  controls: string
  onSelect: () => void
}) {
  const shown = byMetric(entry.ranking, metric)
  const best = shown.rows.find((r) => !r.wrong)
  const own = shown.rows.find((r) => r.timelake)
  const versus = own?.wrong
    ? undefined
    : own && best && best.value > 0
      ? `${(own.value / best.value).toFixed(1)}×`
      : undefined
  return (
    <button
      type="button"
      onClick={onSelect}
      // A selected row on wide screens, a row that opens its results under it on narrow ones.
      aria-current={wide && open ? "true" : undefined}
      aria-expanded={wide ? undefined : open}
      aria-controls={open ? controls : undefined}
      className={cn(
        "grid min-h-11 w-full items-center gap-x-3 rounded-md px-2 py-1.5 text-left transition-colors hover:bg-muted lg:col-start-1",
        columns,
        open && "bg-muted",
      )}
    >
      <span className="flex min-w-0 flex-col">
        <span className={cn("truncate text-xs", entry.id ? "font-mono" : "font-medium")}>{entry.title}</span>
        {entry.description && (
          <span className="line-clamp-1 text-[11px] text-muted-foreground">{entry.description}</span>
        )}
      </span>
      <span className="text-right font-mono text-xs tabular-nums">
        {own ? formatRankValue(own.value, shown.unit) : "—"}
      </span>
      <span
        className="flex items-center justify-end gap-1 text-[11px] text-muted-foreground"
        title={best ? `${VERSUS[metric][1]}: ${best.label}, ${formatRankValue(best.value, shown.unit)}` : undefined}
      >
        {own?.wrong ? (
          <>
            <CircleAlert className="size-3" aria-hidden /> wrong
          </>
        ) : versus ? (
          <span className="font-mono tabular-nums">{versus}</span>
        ) : entry.failed.includes("Timelake") ? (
          "failed"
        ) : (
          "—"
        )}
      </span>
      <ChevronDown
        className={cn("size-3.5 text-muted-foreground transition-transform lg:hidden", open && "rotate-180")}
        aria-hidden
      />
    </button>
  )
}

/** The selected row's results: every reader by the chosen metric, the fastest named, its SQL, and its history. */
function Details({ entry, metric }: { entry: Entry; metric: Metric }) {
  const shown = byMetric(entry.ranking, metric)
  const what =
    metric === "time" ? "First run of every reader" : `${shown.note.charAt(0).toUpperCase()}${shown.note.slice(1)}`
  const note = `${what}; lower is better.`
  // The chart draws the readers the ranking lists, in its order: dashed where not the system's own.
  const lines: HistoryLine[] = entry.ranking.rows
    .filter((r) => entry.points.some((p) => typeof p[r.key] === "number"))
    .map((r) => ({
      engine: r.key,
      label: r.label,
      color: SYSTEM_COLORS[r.system] ?? "var(--chart-2)",
      dashed: !isOwnReader(r.key),
    }))
  return (
    <div className="flex min-w-0 flex-col gap-4">
      <PanelHeader
        title={<span className={entry.id ? "font-mono text-base" : undefined}>{entry.title}</span>}
        note={entry.description ? `${entry.description} ${note}` : note}
      />
      <RankingBars ranking={shown} perSystem={false} queries={entry.id ? 1 : entry.ranking.queries} learnMore="" />
      {entry.failed.length > 0 && (
        <p className="text-xs text-muted-foreground">Failed or timed out: {entry.failed.join(", ")}.</p>
      )}
      {entry.sql && <SqlBlock label="SQL every system runs" sql={entry.sql} />}
      {entry.tuned && <SqlBlock label="Timelake's SQL in a tuned run" sql={entry.tuned} />}
      <div className="flex flex-col gap-2 border-t pt-4">
        <h4 className="text-sm">Time over runs</h4>
        <p className="text-xs text-muted-foreground">
          First-run time in each of the {entry.points.length} {entry.points.length === 1 ? "run" : "runs"} on this
          storage; dashed: a system read by another engine.
        </p>
        {lines.length > 0 && entry.points.length > 1 ? (
          <BenchHistory lines={lines} rows={entry.points} />
        ) : (
          <p className="text-sm text-muted-foreground">One run so far: the history starts with the next.</p>
        )}
      </div>
    </div>
  )
}

/** A query's SQL, folded away under its label. */
function SqlBlock({ label, sql }: { label: string; sql: string }) {
  return (
    <Collapsible>
      <CollapsibleTrigger className="group flex min-h-8 items-center gap-1 text-xs text-muted-foreground hover:text-foreground">
        <ChevronDown className="size-3 transition-transform group-data-[panel-open]:rotate-180" />
        {label}
      </CollapsibleTrigger>
      <CollapsibleContent className="pt-2">
        <pre className="overflow-x-auto rounded-md bg-muted p-3 font-mono text-[11px] leading-relaxed">{sql}</pre>
      </CollapsibleContent>
    </Collapsible>
  )
}
