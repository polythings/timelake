"use client"

import { useEffect, useRef } from "react"

import { DOT_FONT, fitCanvas, resolveColor } from "@/lib/dot-canvas"
import { watchTheme } from "@/lib/theme"
import { cn } from "@/lib/utils"

/** A run of squares in one color; `alpha` dims it (e.g. metadata next to data). */
export type Segment = { count: number; color: string; alpha?: number }

/**
 * Squares on a grid, left to right and top to bottom, one segment after another. Static: it redraws only when its
 * width or segments change. `perRow` fixes the columns; otherwise the grid fills the width.
 */
export function DotGrid({
  segments,
  dot = 3,
  gap = 1,
  perRow,
  className,
  label,
}: {
  segments: Segment[]
  dot?: number
  gap?: number
  perRow?: number
  className?: string
  label?: string
}) {
  const canvas = useRef<HTMLCanvasElement>(null)

  useEffect(() => {
    const c = canvas.current
    const ctx = c?.getContext("2d")
    if (!c || !ctx) return
    const cell = dot + gap
    const draw = () => {
      const width = perRow ? perRow * cell : c.clientWidth
      const cols = Math.max(1, perRow ?? Math.floor((width + gap) / cell))
      const total = segments.reduce((s, x) => s + x.count, 0)
      const rows = Math.max(1, Math.ceil(total / cols))
      fitCanvas(c, ctx, width, rows * cell - gap)
      c.style.height = `${rows * cell - gap}px`
      if (perRow) c.style.width = `${width - gap}px`
      let i = 0
      for (const s of segments) {
        ctx.fillStyle = resolveColor(c, s.color)
        ctx.globalAlpha = s.alpha ?? 1
        for (let n = 0; n < s.count; n++, i++) ctx.fillRect((i % cols) * cell, Math.floor(i / cols) * cell, dot, dot)
      }
      ctx.globalAlpha = 1
    }
    draw()
    const unwatch = watchTheme(draw)
    if (perRow) return unwatch
    const ro = new ResizeObserver(draw)
    ro.observe(c)
    return () => {
      unwatch()
      ro.disconnect()
    }
  }, [segments, dot, gap, perRow])

  return (
    <canvas
      ref={canvas}
      role={label ? "img" : undefined}
      aria-label={label}
      aria-hidden={label ? undefined : true}
      className={cn(perRow ? "block" : "block w-full", className)}
    />
  )
}

/** Text in the dot-matrix font: lit squares in `color`, the rest of each glyph faintly. */
export function DotText({ text, color = "var(--primary)", dot = 4 }: { text: string; color?: string; dot?: number }) {
  return (
    <span role="img" aria-label={text} className="inline-flex gap-[3px]">
      {[...text].map((ch, i) => {
        const glyph = DOT_FONT[ch] ?? DOT_FONT[" "] ?? []
        const cols = glyph[0]?.length ?? 5
        return (
          <span
            key={i}
            aria-hidden
            className="grid gap-px"
            style={{ gridTemplateColumns: `repeat(${cols}, ${dot}px)`, gridAutoRows: `${dot}px` }}
          >
            {glyph.flatMap((row, r) =>
              [...row].map((bit, c) => (
                <span
                  key={`${r}-${c}`}
                  style={{ background: bit === "1" ? color : "var(--dot)", opacity: bit === "1" ? 1 : 0.35 }}
                />
              )),
            )}
          </span>
        )
      })}
    </span>
  )
}
