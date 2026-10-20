"use client"

import { ChevronDown, ChevronRight, CircleAlert } from "lucide-react"
import { useState } from "react"

import { BenchHistory, type HistoryLine } from "@/components/bench-history"
import { Panel, RankingBars, RankingCard, useMetric } from "@/components/ranking-card"
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import QUERIES from "@/lib/bench-queries.json"
import { SYSTEM_COLORS, isOwnReader } from "@/lib/bench-systems"
import type { HistoryPoint, Section } from "@/lib/benchmarks"
import { formatTime } from "@/lib/format"
import { cn } from "@/lib/utils"

/** Each suite's queries as the query files describe them (bench/queries; timebench queries --site). */
const DESCRIBED = QUERIES as Record<string, { name: string; description: string; sql: string }[]>

/**
 * One suite at one checkpoint: the overall ranking next to the list of its queries, each with where Timelake placed;
 * a query opens its own ranking and its history over the runs below.
 */
export function SectionResults({
  section,
  history,
}: {
  section: Section
  /** Query -> one point per run, first run per engine. */
  history: Record<string, HistoryPoint[]>
}) {
  const [open, setOpen] = useState<string | null>(null)
  const query = section.queries.find((q) => q.query === open)
  const described = new Map((DESCRIBED[section.suite] ?? []).map((q) => [q.name, q]))
  const points = query ? (history[query.query] ?? []) : []
  // The chart draws the readers the ranking next to it lists, in its order: dashed where not the system's own.
  const drawn: HistoryLine[] = (query?.ranking.rows ?? [])
    .filter((r) => points.some((p) => typeof p[r.key] === "number"))
    .map((r) => ({
      engine: r.key,
      label: r.label,
      color: SYSTEM_COLORS[r.system] ?? "var(--chart-2)",
      dashed: !isOwnReader(r.key),
    }))
  const about = query ? described.get(query.query) : undefined
  return (
    <div className="flex flex-col gap-4">
      <div className="grid gap-4 lg:grid-cols-2">
        <RankingCard ranking={section.ranking} perSystem={false} learnMore="" />
        <Panel
          title="Per query"
          note="Timelake's first run, and its time over the fastest reader with a right answer (1.0× is the fastest). Open a query for every reader, its SQL and its history."
        >
          <ul className="flex flex-col">
            {section.queries.map((q) => {
              const fastest = q.ranking.rows.find((r) => !r.wrong)
              const own = q.ranking.rows.find((r) => r.timelake)
              const selected = q.query === open
              return (
                <li key={q.query}>
                  <button
                    type="button"
                    onClick={() => setOpen(selected ? null : q.query)}
                    aria-expanded={selected}
                    className={cn(
                      "grid w-full grid-cols-[minmax(0,1fr)_auto_3.5rem_1rem] items-center gap-x-3 rounded-md px-2 py-1.5 text-left transition-colors hover:bg-muted sm:grid-cols-[minmax(0,1fr)_4rem_4rem_1rem]",
                      selected && "bg-muted",
                    )}
                  >
                    <span className="flex min-w-0 flex-col">
                      <span className="truncate font-mono text-xs">{q.query}</span>
                      {described.get(q.query) && (
                        <span className="line-clamp-1 text-[11px] text-muted-foreground">
                          {described.get(q.query)?.description}
                        </span>
                      )}
                    </span>
                    <span className="flex items-center justify-end gap-1 text-[11px] text-muted-foreground">
                      {own?.wrong ? (
                        <>
                          <CircleAlert className="size-3" aria-hidden />{" "}
                          <span className="sr-only sm:not-sr-only">wrong</span>
                        </>
                      ) : own && fastest ? (
                        <span className="font-mono tabular-nums">{(own.value / fastest.value).toFixed(1)}×</span>
                      ) : q.failed.includes("Timelake") ? (
                        "failed"
                      ) : (
                        "not in this run"
                      )}
                    </span>
                    <span className="text-right font-mono text-xs tabular-nums">
                      {own ? formatTime(own.value) : "—"}
                    </span>
                    <ChevronRight
                      className={cn("size-3.5 text-muted-foreground transition-transform", selected && "rotate-90")}
                      aria-hidden
                    />
                  </button>
                </li>
              )
            })}
          </ul>
        </Panel>
      </div>
      {query && (
        <div className="grid gap-4 lg:grid-cols-2">
          <QueryCard key={query.query} query={query} about={about} />
          <Panel
            title="Over time"
            note={`first run in each of the ${points.length} ${points.length === 1 ? "run" : "runs"} on this storage; dashed: a system read by another engine`}
          >
            {drawn.length > 0 && points.length > 1 ? (
              <BenchHistory lines={drawn} rows={points} />
            ) : (
              <p className="text-sm text-muted-foreground">One run so far: the history starts with the next.</p>
            )}
          </Panel>
        </div>
      )}
    </div>
  )
}

/** One query: every reader's result by time (or requests, cost, bytes read where counted), and its SQL. */
function QueryCard({
  query,
  about,
}: {
  query: Section["queries"][number]
  about?: { description: string; sql: string }
}) {
  const { shown, action } = useMetric(query.ranking)
  const note =
    shown.unit === "seconds"
      ? "First run of every reader."
      : `${shown.note.charAt(0).toUpperCase()}${shown.note.slice(1)}.`
  return (
    <Panel
      title={<span className="font-mono text-base">{query.query}</span>}
      note={about ? `${about.description} ${note}` : note}
      action={action}
    >
      <RankingBars ranking={shown} perSystem={false} learnMore="" />
      {query.failed.length > 0 && (
        <p className="text-xs text-muted-foreground">Failed or timed out: {query.failed.join(", ")}.</p>
      )}
      {about && (
        <Collapsible>
          <CollapsibleTrigger className="group flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground">
            <ChevronDown className="size-3 transition-transform group-data-[panel-open]:rotate-180" />
            SQL, as the Timelake reader runs it
          </CollapsibleTrigger>
          <CollapsibleContent className="pt-2">
            <pre className="overflow-x-auto rounded-md bg-muted p-3 font-mono text-[11px] leading-relaxed">
              {about.sql}
            </pre>
          </CollapsibleContent>
        </Collapsible>
      )}
    </Panel>
  )
}
