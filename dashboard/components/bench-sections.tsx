"use client"

import { ChevronRight, CircleAlert } from "lucide-react"
import { useState } from "react"

import { BenchHistory, type HistoryLine } from "@/components/bench-history"
import { Panel, RankingBars, RankingCard } from "@/components/ranking-card"
import type { HistoryPoint, Section } from "@/lib/benchmarks"
import { formatTime } from "@/lib/format"
import { cn } from "@/lib/utils"

const place = (n: number) => `${n}${n === 1 ? "st" : n === 2 ? "nd" : n === 3 ? "rd" : "th"}`

/**
 * One suite at one checkpoint: the overall ranking next to the list of its queries, each with where Timelake placed;
 * a query opens its own ranking and its history over the runs below.
 */
export function SectionResults({
  section,
  history,
  lines,
}: {
  section: Section
  /** Query -> one point per run, first run per engine. */
  history: Record<string, HistoryPoint[]>
  /** The engines drawn in history charts: each system's own reader. */
  lines: HistoryLine[]
}) {
  const [open, setOpen] = useState<string | null>(null)
  const query = section.queries.find((q) => q.query === open)
  const points = query ? (history[query.query] ?? []) : []
  const drawn = lines.filter((l) => points.some((p) => typeof p[l.engine] === "number"))
  return (
    <div className="flex flex-col gap-4">
      <div className="grid gap-4 lg:grid-cols-2">
        <RankingCard ranking={section.ranking} perSystem={false} />
        <Panel
          title="Per query"
          note="Timelake's first run and its place among the systems; open a query for all of them"
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
                    <span className="truncate font-mono text-xs">{q.query}</span>
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
          <Panel title={<span className="font-mono text-base">{query.query}</span>} note="first run, every reader">
            <RankingBars ranking={query.ranking} perSystem={false} />
            {query.failed.length > 0 && (
              <p className="text-xs text-muted-foreground">Failed or timed out: {query.failed.join(", ")}.</p>
            )}
          </Panel>
          <Panel
            title="Over time"
            note={`first run in each of the ${points.length} ${points.length === 1 ? "run" : "runs"} on this storage`}
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
