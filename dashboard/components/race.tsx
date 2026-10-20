"use client"

import { useEffect, useRef } from "react"

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

const GAP = 1

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
  const value = racer.value.toLocaleString("en", { minimumFractionDigits: decimals, maximumFractionDigits: decimals })

  useEffect(() => {
    const c = canvas.current
    if (!c) return
    const width = c.clientWidth
    const perRow = Math.max(1, Math.floor(width / (dot + GAP)))
    const rows = Math.ceil(racer.dots / perRow)
    const dpr = window.devicePixelRatio || 1
    c.width = width * dpr
    c.height = rows * (dot + GAP) * dpr
    c.style.height = `${rows * (dot + GAP)}px`
    const ctx = c.getContext("2d")
    if (!ctx) return
    ctx.scale(dpr, dpr)
    const css = getComputedStyle(c)
    const draw = () => {
      ctx.fillStyle = css.getPropertyValue(/^var\((--[\w-]+)\)$/.exec(racer.color)?.[1] ?? "--muted-foreground").trim()
      for (let i = 0; i < racer.dots; i++) {
        ctx.fillRect((i % perRow) * (dot + GAP), Math.floor(i / perRow) * (dot + GAP), dot, dot)
      }
    }
    draw()
    return watchTheme(draw)
  }, [racer, dot])

  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap items-baseline justify-between gap-x-4 text-sm">
        <span className={racer.timelake ? "font-medium" : "text-muted-foreground"}>{racer.label}</span>
        <span className="font-mono text-xs text-muted-foreground tabular-nums">
          <span className="text-foreground">{value}</span>
          {` ${unit} · ${racer.seconds.toFixed(2)} s`}
        </span>
      </div>
      <canvas ref={canvas} className="w-full" />
    </div>
  )
}
