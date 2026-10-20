"use client"

import { useState } from "react"
import { CartesianGrid, Line, LineChart, XAxis, YAxis, type TooltipContentProps } from "recharts"

import { ChartTooltipCard } from "@/components/chart-tooltip-card"
import { ChartContainer, ChartTooltip, type ChartConfig } from "@/components/ui/chart"
import { formatTime } from "@/lib/format"
import { cn } from "@/lib/utils"

/**
 * One line per engine; its color is its system's (lib/bench-systems.ts, SYSTEM_COLORS), whatever else is shown, and a
 * system's other readers are dashed.
 */
export type HistoryLine = { engine: string; label: string; color: string; dashed?: boolean }
export type HistoryRow = { date: string; commit: string; run?: string } & Record<string, number | string | undefined>

type Point = { date: string; commit: string; tooltip: [string, string][] }

function HistoryTooltip({ active, payload }: TooltipContentProps) {
  const point = active ? (payload[0]?.payload as Point | undefined) : undefined
  if (!point) return null
  return <ChartTooltipCard title={`${point.date} · ${point.commit}`} rows={point.tooltip} />
}

/** A run's label on the axis: its date, with the time where two runs share a date ("2026-10-18 06:39"). */
function runLabels(rows: HistoryRow[]): string[] {
  const dates = rows.map((r) => r.date)
  return rows.map((r) => {
    const time = typeof r.run === "string" ? /T(\d{2})(\d{2})/.exec(r.run) : null
    return dates.filter((d) => d === r.date).length > 1 && time ? `${r.date} ${time[1]}:${time[2]}` : r.date
  })
}

export function BenchHistory({ lines, rows }: { lines: HistoryLine[]; rows: HistoryRow[] }) {
  // A line picked in the legend (hover, focus, or tap) stands out; the others fade.
  const [picked, setPicked] = useState<number | null>(null)
  // Engine names contain "+", which CSS variable names can't: the chart keys lines by position.
  const keys = lines.map((_, i) => `e${i}`)
  const config = Object.fromEntries(
    lines.map((l, i) => [keys[i], { label: l.label, color: l.color }]),
  ) satisfies ChartConfig
  const labels = runLabels(rows)
  const data = rows.map((r, at) => ({
    date: labels[at],
    commit: r.commit.slice(0, 7),
    ...Object.fromEntries(lines.map((l, i) => [keys[i], r[l.engine]])),
    tooltip: lines.flatMap((l) => {
      const v = r[l.engine]
      return typeof v === "number" ? [[l.label, formatTime(v)] as [string, string]] : []
    }),
  }))
  const faded = (i: number) => picked !== null && picked !== i

  return (
    <div className="flex flex-col gap-3">
      <ChartContainer config={config} className="aspect-auto h-72 w-full">
        <LineChart data={data} margin={{ left: 8, right: 16, top: 8 }}>
          <CartesianGrid vertical={false} />
          <XAxis dataKey="date" tickLine={false} axisLine={false} tickMargin={8} />
          <YAxis
            scale="log"
            domain={["auto", "auto"]}
            tickFormatter={(v: number) => formatTime(v)}
            tickLine={false}
            axisLine={false}
            width={64}
          />
          <ChartTooltip content={HistoryTooltip} />
          {keys.map((k, i) => (
            <Line
              key={k}
              dataKey={k}
              stroke={`var(--color-${k})`}
              strokeWidth={picked === i ? 3 : 2}
              strokeOpacity={faded(i) ? 0.15 : 1}
              strokeDasharray={lines[i]?.dashed ? "5 4" : undefined}
              dot={{ r: 4, fill: `var(--color-${k})`, fillOpacity: faded(i) ? 0.15 : 1, strokeWidth: 0 }}
              connectNulls
              isAnimationActive={false}
            />
          ))}
        </LineChart>
      </ChartContainer>
      <ul className="flex flex-wrap justify-center gap-x-1 gap-y-0.5 text-xs" onMouseLeave={() => setPicked(null)}>
        {lines.map((l, i) => (
          <li key={l.engine}>
            <button
              type="button"
              aria-pressed={picked === i}
              onMouseEnter={() => setPicked(i)}
              onFocus={() => setPicked(i)}
              onBlur={() => setPicked(null)}
              onClick={() => setPicked(picked === i ? null : i)}
              className={cn(
                "flex min-h-8 items-center gap-1.5 rounded-md px-2 transition-opacity hover:bg-muted focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none",
                faded(i) && "opacity-40",
              )}
            >
              <svg width="18" height="8" aria-hidden className="shrink-0">
                <line
                  x1="1"
                  x2="17"
                  y1="4"
                  y2="4"
                  stroke={l.color}
                  strokeWidth="2"
                  strokeDasharray={l.dashed ? "4 3" : undefined}
                />
              </svg>
              {l.label}
            </button>
          </li>
        ))}
      </ul>
    </div>
  )
}
