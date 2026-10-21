"use client"

import { useEffect, useRef } from "react"

import { GAP, fitCanvas, resolveColor } from "@/lib/dot-canvas"
import { formatNumber } from "@/lib/format"
import { watchTheme } from "@/lib/theme"

/** One system in a race: how much it ends up with (`value`, drawn as `dots`), and how long that took. */
export type Racer = {
  label: string
  value: number
  dots: number
  seconds: number
  timelake: boolean
  /** The system's chart color, e.g. "var(--chart-3)". */
  color: string
}

/**
 * Systems side by side, each a lane of dots with its total and how long it took. What a dot stands for is in `unit`
 * and the caption.
 */
export function Race({
  title,
  racers,
  unit,
  decimals = 0,
  caption,
  dot = 3,
}: {
  title?: string
  racers: Racer[]
  unit: string
  decimals?: number
  caption: string
  /** Pixels per square. */
  dot?: number
}) {
  return (
    <div className="flex flex-col gap-5 rounded-lg border bg-card p-6">
      {title ? <h3 className="text-lg font-normal">{title}</h3> : null}
      {racers.map((r) => (
        <Lane key={r.label} racer={r} unit={unit} decimals={decimals} dot={dot} />
      ))}
      <p className="max-w-3xl border-t pt-4 text-xs text-muted-foreground">{caption}</p>
    </div>
  )
}

function Lane({ racer, unit, decimals, dot }: { racer: Racer; unit: string; decimals: number; dot: number }) {
  const canvas = useRef<HTMLCanvasElement>(null)

  // Lays the lane out to its width and paints it, again when the width or the theme changes.
  useEffect(() => {
    const c = canvas.current
    const ctx = c?.getContext("2d")
    if (!c || !ctx) return
    const cell = dot + GAP
    const draw = () => {
      const width = c.clientWidth
      // Hidden (in a closed tab): drawn once it shows.
      if (width === 0) return
      const perRow = Math.max(1, Math.floor(width / cell))
      const height = Math.ceil(racer.dots / perRow) * cell
      c.style.height = `${height}px`
      fitCanvas(c, ctx, width, height)
      ctx.fillStyle = resolveColor(c, racer.color)
      for (let i = 0; i < racer.dots; i++) ctx.fillRect((i % perRow) * cell, Math.floor(i / perRow) * cell, dot, dot)
    }
    draw()
    const ro = new ResizeObserver(draw)
    ro.observe(c)
    const unwatch = watchTheme(draw)
    return () => {
      ro.disconnect()
      unwatch()
    }
  }, [racer, dot])

  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap items-baseline justify-between gap-x-4 text-sm">
        <span className={racer.timelake ? "font-medium" : "text-muted-foreground"}>{racer.label}</span>
        <span className="font-mono text-xs text-muted-foreground tabular-nums">
          <span className="text-foreground">{formatNumber(racer.value, decimals)}</span>
          {` ${unit} · ${formatNumber(racer.seconds, 2)} s`}
        </span>
      </div>
      <canvas ref={canvas} className="w-full" />
    </div>
  )
}
