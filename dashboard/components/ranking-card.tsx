import { CircleAlert } from "lucide-react"

import { DotGrid } from "@/components/dots"
import { type Rank, type Ranking, SYSTEM_COLORS } from "@/lib/bench-systems"
import { formatCompact, formatTime } from "@/lib/format"

/** Columns of squares in the longest bar of a card. */
const SCALE = 40
/** Results within 5% of the fastest get no multiplier. */
const SAME_AS_FASTEST = 1.05

/** A card in the benchmark pages' style: a title, one muted note, then its content. */
export function Panel({
  title,
  note,
  children,
  className,
}: {
  title: React.ReactNode
  note?: React.ReactNode
  children: React.ReactNode
  className?: string
}) {
  return (
    <div className={`flex min-w-0 flex-col gap-4 rounded-lg border bg-card p-6 ${className ?? ""}`}>
      <div className="flex flex-col gap-0.5">
        <h3 className="text-lg font-normal">{title}</h3>
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
}: {
  ranking: Ranking
  perSystem?: boolean
  /** How many queries a row's value is over, for "2 of 5 wrong". */
  queries?: number
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
  const show = (r: Rank) =>
    ranking.unit === "rows/s"
      ? `${formatCompact(r.value)}/s`
      : formatTime(ranking.unit === "seconds" ? r.value : r.value / 1000)
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
            <span
              className={r.timelake ? "truncate text-sm" : "truncate text-sm text-muted-foreground"}
              title={[
                r.label,
                ...(r.repeat === undefined ? [] : [`best repeat ${formatTime(r.repeat)}`]),
                ...(r.requests === undefined ? [] : [`${Math.round(r.requests)} requests`]),
              ].join(" · ")}
            >
              {r.label}
            </span>
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
                <span className="font-mono text-[11px] text-muted-foreground tabular-nums">{ratio.toFixed(1)}×</span>
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
          <p className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <CircleAlert className="size-3.5 shrink-0" aria-hidden />
            Wrong answers, shown apart
          </p>
          {list(wrong)}
        </div>
      )}
    </div>
  )
}

/** One ranking as a card. */
export function RankingCard({ ranking, perSystem = true }: { ranking: Ranking; perSystem?: boolean }) {
  return (
    <Panel title={ranking.title} note={`${ranking.note}; ${ranking.higherIsBetter ? "higher" : "lower"} is better`}>
      <RankingBars ranking={ranking} perSystem={perSystem} queries={ranking.queries} />
    </Panel>
  )
}
