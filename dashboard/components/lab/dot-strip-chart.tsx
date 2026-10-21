"use client"

import { useEffect, useRef } from "react"

import { generated, looped } from "@/components/lab/strip-chart"
import { CELL, DOT, cssVar, fitCanvas } from "@/lib/dot-canvas"

/** Columns between the pen and the right edge. */
const MARGIN = 10

/**
 * The strip-chart recorder on a dot grid: the paper is dim squares, the trace lights the squares it passes through, and
 * the area under it glows faintly. Without `data` the pen draws generated waves; with it, it plays the values in a loop.
 */
export function DotStripChart({
  data,
  children,
  tick = 70,
  paperAlpha = 1,
}: {
  data?: number[] | null
  children?: React.ReactNode
  /** Milliseconds per new column. */
  tick?: number
  /** How bright the background squares are, from 0 (none) to 1. */
  paperAlpha?: number
}) {
  const canvas = useRef<HTMLCanvasElement>(null)

  useEffect(() => {
    const c = canvas.current
    const ctx = c?.getContext("2d")
    if (!c || !ctx || data === null) return
    const source = data && data.length > 0 ? looped(data) : generated()
    const [lo, hi] = data && data.length > 0 ? [Math.min(...data), Math.max(...data)] : [-1, 1]
    let cols = 0
    let rows = 0
    let trace: number[] = []
    const resize = () => {
      fitCanvas(c, ctx, c.clientWidth, c.clientHeight)
      cols = Math.floor(c.clientWidth / CELL)
      rows = Math.floor(c.clientHeight / CELL)
      while (trace.length < cols - MARGIN) trace.push(source.next().value as number)
    }
    resize()
    const ro = new ResizeObserver(resize)
    ro.observe(c)

    const color = (name: string) => cssVar(c, name)
    // The row a value lands on: the top and bottom fifth of the strip stay clear.
    const row = (v: number) => Math.round((1 - (hi === lo ? 0.5 : (v - lo) / (hi - lo))) * rows * 0.6 + rows * 0.2)

    const draw = () => {
      // The theme's colors, read each frame so a theme change shows.
      const [paper, under, pen] = [color("--border"), color("--muted-foreground"), color("--primary")]
      ctx.clearRect(0, 0, c.clientWidth, c.clientHeight)
      const penCol = cols - MARGIN
      const first = penCol - trace.length
      let prev: number | null = null
      for (let x = 0; x < cols; x++) {
        const i = x - first
        const v = i >= 0 && i < trace.length ? trace[i] : undefined
        const y = v === undefined ? null : row(v)
        // From the previous column's row to this one's, so steep moves stay joined.
        const [top, bottom] =
          y === null ? [rows, rows] : prev === null ? [y, y] : [Math.min(prev, y), Math.max(prev, y)]
        for (let r = 0; r < rows; r++) {
          const onLine = y !== null && r >= top && r <= bottom
          ctx.globalAlpha = onLine ? 1 : y !== null && r > bottom ? 0.22 : paperAlpha
          ctx.fillStyle = onLine ? pen : y !== null && r > bottom ? under : paper
          ctx.fillRect(x * CELL, r * CELL, DOT, DOT)
        }
        // The pen's column: a faint vertical guide.
        if (x === penCol) {
          ctx.globalAlpha = 0.35
          ctx.fillStyle = under
          for (let r = 0; r < rows; r++)
            if (y === null || r < top || r > bottom) ctx.fillRect(x * CELL, r * CELL, DOT, DOT)
        }
        prev = y
      }
      ctx.globalAlpha = 1
    }
    draw()
    const timer = setInterval(() => {
      trace = [...trace.slice(-(cols - MARGIN) + 1), source.next().value as number]
      draw()
    }, tick)
    return () => {
      clearInterval(timer)
      ro.disconnect()
    }
  }, [data, tick, paperAlpha])

  return (
    <div className="relative h-[234px] overflow-hidden">
      <canvas ref={canvas} aria-hidden className="absolute inset-0 h-full w-full" />
      {children}
    </div>
  )
}
