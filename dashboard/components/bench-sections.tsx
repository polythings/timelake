"use client"

import { ChevronDown, ChevronRight, CircleAlert } from "lucide-react"
import { useState } from "react"

import { BenchHistory, type HistoryLine } from "@/components/bench-history"
import { Panel, RankingBars, RankingCard } from "@/components/ranking-card"
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import QUERIES from "@/lib/bench-queries.json"
import { SYSTEM_COLORS, isOwnReader } from "@/lib/bench-systems"
import type { HistoryPoint, Section } from "@/lib/benchmarks"
import { formatTime } from "@/lib/format"
import { cn } from "@/lib/utils"

/** Each suite's queries as the query files describe them (bench/queries; timebench queries --site). */
const DESCRIBED = QUERIES as Record<string, { name: string; description: string; sql: string }[]>

const place = (n: number) => `${n}${n === 1 ? "st" : n === 2 ? "nd" : n === 3 ? "rd" : "th"}`

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
        <RankingCard ranking={section.ranking} perSystem={false} />
        <Panel
          title="Per query"
          note="Timelake's first run and its place among the readers with right answers; open a query for all of them, its SQL and history"
        >
          <ul className="flex flex-col">
            {section.queries.map((q) => {
              const right = q.ranking.rows.filter((r) => !r.wrong)
              const at = right.findIndex((r) => r.timelake)
              const own = q.ranking.rows.find((r) => r.timelake)
              const selected = q.query === open
              return (
                <li key={q.query}>
                  <button
                    type="button"
                    onClick={() => setOpen(selected ? null : q.query)}
                    aria-expanded={selected}
                    className={cn(
                      "grid w-full grid-cols-[minmax(0,1fr)_auto_4.5rem_1rem] items-center gap-x-3 rounded-md px-2 py-1.5 text-left transition-colors hover:bg-muted",
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
                    <span className="flex items-center gap-1 text-[11px] text-muted-foreground">
                      {own?.wrong ? (
                        <>
                          <CircleAlert className="size-3" aria-hidden /> wrong
                        </>
                      ) : at >= 0 ? (
                        `${place(at + 1)} of ${right.length}`
                      ) : (
                        "failed"
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
          <Panel
            title={<span className="font-mono text-base">{query.query}</span>}
            note={about ? `${about.description} First run of every reader.` : "First run of every reader."}
          >
            <RankingBars ranking={query.ranking} perSystem={false} />
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
