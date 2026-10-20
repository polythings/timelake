"use client"

import { ChevronDown, ChevronRight, CircleAlert } from "lucide-react"
import { Fragment, useId, useState } from "react"

import { BenchHistory, type HistoryLine } from "@/components/bench-history"
import { PanelHeader, RankingBars, useMetric } from "@/components/ranking-card"
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import QUERIES from "@/lib/bench-queries.json"
import { type Ranking, SYSTEM_COLORS, isOwnReader } from "@/lib/bench-systems"
import type { HistoryPoint, Section, SectionHistory } from "@/lib/benchmarks"
import { formatTime } from "@/lib/format"
import { cn } from "@/lib/utils"

/** Each suite's queries as the query files describe them (bench/queries; timebench queries --site). */
const DESCRIBED = QUERIES as Record<string, { name: string; description: string; sql: string }[]>

/** Where the results move beside the list (Tailwind's lg). */
const WIDE = "(min-width: 64rem)"

/** One row of a section's list: all its queries together, or one query. */
type Entry = {
  id: string
  title: string
  description?: string
  sql?: string
  ranking: Ranking
  failed: string[]
  points: HistoryPoint[]
}

/**
 * One suite at one checkpoint, as a list and the selected row's results: all queries together first, then each query
 * with Timelake's time and how it compares with the fastest right answer. On wide screens the results sit to the right
 * of the list; on narrow ones they open under the selected row.
 */
export function SectionResults({ section, history }: { section: Section; history: SectionHistory }) {
  const described = new Map((DESCRIBED[section.suite] ?? []).map((q) => [q.name, q]))
  const entries: Entry[] = [
    {
      id: "",
      title: "All queries",
      description: `The geometric mean of the ${section.queries.length} queries' first runs.`,
      ranking: section.ranking,
      failed: [],
      points: history.all,
    },
    ...section.queries.map((q) => ({
      id: q.query,
      title: q.query,
      description: described.get(q.query)?.description,
      sql: described.get(q.query)?.sql,
      ranking: q.ranking,
      failed: q.failed,
      points: history.queries[q.query] ?? [],
    })),
  ]
  // Wide screens always show one row's results beside the list; narrow ones open and close them under the row.
  const [selected, setSelected] = useState<string | null>("")
  const select = (id: string) => setSelected(id === selected && !window.matchMedia(WIDE).matches ? null : id)
  const panel = useId()
  return (
    <div
      className="grid gap-x-6 rounded-lg border bg-card p-4 sm:p-6 lg:grid-cols-[minmax(0,5fr)_minmax(0,7fr)] lg:[grid-template-rows:repeat(var(--rows),auto)_1fr]"
      style={{ "--rows": entries.length + 1 } as React.CSSProperties}
    >
      <div className="grid grid-cols-[minmax(0,1fr)_3.5rem_4rem_1rem] gap-x-3 px-2 pb-2 text-[11px] text-muted-foreground lg:col-start-1">
        <span>Query</span>
        <span className="text-right">vs fastest</span>
        <span className="text-right">Timelake</span>
      </div>
      {entries.map((e) => {
        const open = e.id === selected
        return (
          <Fragment key={e.id}>
            <Row entry={e} open={open} controls={panel} onSelect={() => select(e.id)} />
            {open && (
              <div
                id={panel}
                role="region"
                aria-label={e.title}
                className="my-2 rounded-md border p-4 lg:sticky lg:top-20 lg:col-start-2 lg:row-[1/-1] lg:my-0 lg:self-start lg:rounded-none lg:border-0 lg:border-l lg:py-0 lg:pr-0 lg:pl-6"
              >
                <Details key={e.id} entry={e} />
              </div>
            )}
          </Fragment>
        )
      })}
    </div>
  )
}

/** A list row: the query and its description, how Timelake compares with the fastest right answer, and its time. */
function Row({
  entry,
  open,
  controls,
  onSelect,
}: {
  entry: Entry
  open: boolean
  controls: string
  onSelect: () => void
}) {
  const fastest = entry.ranking.rows.find((r) => !r.wrong)
  const own = entry.ranking.rows.find((r) => r.timelake)
  return (
    <button
      type="button"
      onClick={onSelect}
      aria-expanded={open}
      aria-controls={open ? controls : undefined}
      className={cn(
        "grid w-full grid-cols-[minmax(0,1fr)_3.5rem_4rem_1rem] items-center gap-x-3 rounded-md px-2 py-1.5 text-left transition-colors hover:bg-muted lg:col-start-1",
        open && "bg-muted",
      )}
    >
      <span className="flex min-w-0 flex-col">
        <span className={cn("truncate text-xs", entry.id ? "font-mono" : "font-medium")}>{entry.title}</span>
        {entry.description && (
          <span className="line-clamp-1 text-[11px] text-muted-foreground">{entry.description}</span>
        )}
      </span>
      <span className="flex items-center justify-end gap-1 text-[11px] text-muted-foreground">
        {own?.wrong ? (
          <>
            <CircleAlert className="size-3" aria-hidden /> wrong
          </>
        ) : own && fastest ? (
          <span className="font-mono tabular-nums">{(own.value / fastest.value).toFixed(1)}×</span>
        ) : entry.failed.includes("Timelake") ? (
          "failed"
        ) : (
          "—"
        )}
      </span>
      <span className="text-right font-mono text-xs tabular-nums">{own ? formatTime(own.value) : "—"}</span>
      <ChevronRight
        className={cn("size-3.5 text-muted-foreground transition-transform", open && "rotate-90 lg:rotate-0")}
        aria-hidden
      />
    </button>
  )
}

/** The selected row's results: every reader by time (or requests, cost, bytes read where counted), SQL, history. */
function Details({ entry }: { entry: Entry }) {
  const { shown, action } = useMetric(entry.ranking)
  const note =
    shown.unit === "seconds"
      ? "First run of every reader."
      : `${shown.note.charAt(0).toUpperCase()}${shown.note.slice(1)}.`
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
        action={action}
      />
      <RankingBars ranking={shown} perSystem={false} queries={entry.id ? 1 : entry.ranking.queries} learnMore="" />
      {entry.failed.length > 0 && (
        <p className="text-xs text-muted-foreground">Failed or timed out: {entry.failed.join(", ")}.</p>
      )}
      {entry.sql && (
        <Collapsible>
          <CollapsibleTrigger className="group flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground">
            <ChevronDown className="size-3 transition-transform group-data-[panel-open]:rotate-180" />
            SQL, as the Timelake reader runs it
          </CollapsibleTrigger>
          <CollapsibleContent className="pt-2">
            <pre className="overflow-x-auto rounded-md bg-muted p-3 font-mono text-[11px] leading-relaxed">
              {entry.sql}
            </pre>
          </CollapsibleContent>
        </Collapsible>
      )}
      <div className="flex flex-col gap-2 border-t pt-4">
        <h4 className="text-sm">Over time</h4>
        <p className="text-xs text-muted-foreground">
          First run in each of the {entry.points.length} {entry.points.length === 1 ? "run" : "runs"} on this storage;
          dashed: a system read by another engine.
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
