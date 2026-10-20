"use client"

import { useEffect, useRef } from "react"

import { watchTheme } from "@/lib/theme"
import { cn } from "@/lib/utils"

/** A run of squares in one color; `alpha` dims it (e.g. metadata next to data). */
export type Segment = { count: number; color: string; alpha?: number }

/** "var(--chart-3)" → the color it stands for here; other colors pass through. */
function resolve(el: Element, color: string): string {
  const m = /^var\((--[\w-]+)\)$/.exec(color)
  return m?.[1] ? getComputedStyle(el).getPropertyValue(m[1]).trim() : color
}

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
      const dpr = window.devicePixelRatio || 1
      c.width = width * dpr
      c.height = (rows * cell - gap) * dpr
      c.style.height = `${rows * cell - gap}px`
      if (perRow) c.style.width = `${width - gap}px`
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
      let i = 0
      for (const s of segments) {
        ctx.fillStyle = resolve(c, s.color)
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

/** 5×7 dot-matrix glyphs, top row first. */
const FONT: Record<string, string[]> = {
  "0": ["01110", "10001", "10011", "10101", "11001", "10001", "01110"],
  "1": ["00100", "01100", "00100", "00100", "00100", "00100", "01110"],
  "2": ["01110", "10001", "00001", "00010", "00100", "01000", "11111"],
  "3": ["11110", "00001", "00001", "01110", "00001", "00001", "11110"],
  "4": ["00010", "00110", "01010", "10010", "11111", "00010", "00010"],
  "5": ["11111", "10000", "11110", "00001", "00001", "10001", "01110"],
  "6": ["00110", "01000", "10000", "11110", "10001", "10001", "01110"],
  "7": ["11111", "00001", "00010", "00100", "01000", "01000", "01000"],
  "8": ["01110", "10001", "10001", "01110", "10001", "10001", "01110"],
  "9": ["01110", "10001", "10001", "01111", "00001", "00010", "01100"],
  ".": ["00000", "00000", "00000", "00000", "00000", "01100", "01100"],
  ",": ["00000", "00000", "00000", "00000", "01100", "00100", "01000"],
  " ": ["000", "000", "000", "000", "000", "000", "000"],
  "–": ["00000", "00000", "00000", "11111", "00000", "00000", "00000"],
  "×": ["00000", "00000", "10001", "01010", "00100", "01010", "10001"],
  "%": ["11001", "11010", "00010", "00100", "01000", "01011", "10011"],
  m: ["00000", "00000", "11010", "10101", "10101", "10101", "10101"],
  s: ["00000", "00000", "01111", "10000", "01110", "00001", "11110"],
  r: ["00000", "00000", "10110", "11001", "10000", "10000", "10000"],
  e: ["00000", "00000", "01110", "10001", "11111", "10000", "01110"],
  q: ["00000", "00000", "01101", "10011", "01111", "00001", "00001"],
  k: ["10000", "10000", "10010", "10100", "11000", "10100", "10010"],
  B: ["11110", "10001", "10001", "11110", "10001", "10001", "11110"],
  M: ["10001", "11011", "10101", "10101", "10001", "10001", "10001"],
}

/** Text in the dot-matrix font: lit squares in `color`, the rest of each glyph faintly. */
export function DotText({ text, color = "var(--primary)", dot = 4 }: { text: string; color?: string; dot?: number }) {
  return (
    <span role="img" aria-label={text} className="inline-flex gap-[3px]">
      {[...text].map((ch, i) => {
        const glyph = FONT[ch] ?? FONT[" "] ?? []
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
