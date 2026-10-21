"use client"

import { useEffect, useRef } from "react"

import { CELL, DOT, fitCanvas, resolveColor } from "@/lib/dot-canvas"
import { watchTheme } from "@/lib/theme"

/** The picture each card draws, in squares: the use cases, then the benefits. */
export type ArtKind =
  | "sensors"
  | "fleet"
  | "robot"
  | "market"
  | "metrics"
  | "dashboard"
  | "ml"
  | "functions"
  | "agents"
  | "serverless"
  | "layout"
  | "fast"
  | "cheap"
  | "stores"
  | "formats"

/** Columns in a picture, unless a wider one is asked for. */
const COLS = 64
const ROWS = 14
/** Milliseconds to draw the picture in, left to right. */
const REVEAL_MS = 1100

type Cell = { x: number; y: number; color: string; alpha: number }

/** A small seeded random generator (mulberry32), so every visit draws the same picture. */
function seeded(seed: number) {
  let a = seed
  return () => {
    a = (a + 0x6d2b79f5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

const row = (v: number) => Math.max(0, Math.min(ROWS - 1, Math.round(v)))

/** A line through one value per column, joined vertically so steep steps stay connected. */
function line(values: number[], color: string, alpha = 1): Cell[] {
  const cells: Cell[] = []
  values.forEach((v, x) => {
    const y = row(v)
    const prev = x > 0 ? row(values[x - 1] ?? v) : y
    for (let r = Math.min(prev, y); r <= Math.max(prev, y); r++) cells.push({ x, y: r, color, alpha })
  })
  return cells
}

const PICTURES: Record<ArtKind, (cols: number) => Cell[]> = {
  sensors: (cols) => {
    const rnd = seeded(1)
    return [0, 1, 2].flatMap((k) =>
      line(
        Array.from({ length: cols }, (_, x) => 3 + k * 4 + Math.sin(x / 7 + k * 1.7) * 1.6 + (rnd() - 0.5)),
        k === 1 ? "var(--primary)" : "var(--muted-foreground)",
        k === 1 ? 1 : 0.6,
      ),
    )
  },
  fleet: (cols) => {
    const rnd = seeded(2)
    let y = 7
    const route = Array.from(
      { length: cols },
      () => (y = Math.max(2, Math.min(11, y + (rnd() < 0.3 ? (rnd() < 0.5 ? -1 : 1) : 0)))),
    )
    const stops = route.flatMap((v, x) =>
      x % 12 === 6
        ? [-1, 0, 1].flatMap((dx) =>
            [-1, 0, 1].map((dy) => ({ x: x + dx, y: row(v + dy), color: "var(--primary)", alpha: 1 })),
          )
        : [],
    )
    return [...line(route, "var(--muted-foreground)", 0.6), ...stops]
  },
  robot: (cols) => [
    ...line(
      Array.from({ length: cols }, (_, x) => 6.5 + Math.sin(x / 5) * 4.5),
      "var(--primary)",
    ),
    ...line(
      Array.from({ length: cols }, (_, x) => 6.5 + Math.sin(x / 5 + 2.2) * 3),
      "var(--chart-4)",
      0.8,
    ),
  ],
  market: (cols) => {
    const rnd = seeded(4)
    let price = 7
    const cells: Cell[] = []
    for (let x = 1; x < cols - 2; x += 4) {
      const open = price
      price = Math.max(2, Math.min(11, price + (rnd() - 0.48) * 4))
      const [top, bottom] = [Math.min(open, price), Math.max(open, price)]
      const color = price <= open ? "var(--chart-3)" : "var(--chart-5)"
      for (let r = row(top - 1 - rnd() * 1.5); r <= row(bottom + 1 + rnd() * 1.5); r++)
        cells.push({ x: x + 1, y: r, color, alpha: 0.6 })
      for (let r = row(top); r <= row(bottom); r++)
        for (const dx of [0, 1, 2]) cells.push({ x: x + dx, y: r, color, alpha: 1 })
    }
    return cells
  },
  metrics: (cols) => {
    const rnd = seeded(5)
    const cells: Cell[] = []
    for (let y = 1; y < ROWS - 1; y++) {
      let load = rnd()
      for (let x = 0; x < cols; x++) {
        load = Math.max(0, Math.min(1, load + (rnd() - 0.5) * 0.25))
        if (load > 0.35) cells.push({ x, y, color: "var(--primary)", alpha: (load - 0.35) * 1.5 })
      }
    }
    return cells
  },
  ml: () => {
    const rnd = seeded(6)
    const gauss = () => (rnd() + rnd() + rnd() - 1.5) * 2
    return [
      [14, 4, "var(--primary)"],
      [34, 9, "var(--chart-3)"],
      [52, 5, "var(--chart-4)"],
    ].flatMap(([cx, cy, color]) =>
      Array.from({ length: 34 }, () => ({
        x: Math.round(Number(cx) + gauss() * 3),
        y: row(Number(cy) + gauss()),
        color: String(color),
        alpha: 0.9,
      })),
    )
  },
  functions: (cols) => {
    const rnd = seeded(7)
    const cells: Cell[] = []
    for (let y = 1; y < ROWS - 1; y += 2) {
      for (let x = Math.floor(rnd() * 6); x < cols;) {
        const length = 2 + Math.floor(rnd() * 7)
        for (let dx = 0; dx < length && x + dx < cols; dx++)
          cells.push({ x: x + dx, y, color: "var(--primary)", alpha: 0.85 })
        x += length + 2 + Math.floor(rnd() * 10)
      }
    }
    return cells
  },
  dashboard: () => {
    // Two panels: hourly bars, then a line.
    const rnd = seeded(9)
    const cells: Cell[] = []
    let v = 6
    for (let x = 1; x < 30; x += 2) {
      v = Math.max(3, Math.min(11, v + (rnd() - 0.5) * 4))
      for (let y = ROWS - 2; y >= ROWS - 1 - Math.round(v); y--)
        cells.push({ x, y, color: "var(--primary)", alpha: 0.85 })
    }
    const trend = Array.from({ length: 30 }, (_, i) => 8 - Math.sin(i / 5) * 3 - i / 10 + (rnd() - 0.5))
    trend.forEach((t, i) => cells.push({ x: 34 + i, y: row(t), color: "var(--chart-3)", alpha: 1 }))
    return cells
  },
  agents: (cols) => {
    const rnd = seeded(8)
    const cells: Cell[] = []
    for (let y = 0; y < ROWS; y++)
      for (let x = 0; x < cols; x++)
        if (rnd() < 0.14) cells.push({ x, y, color: "var(--primary)", alpha: 0.4 + rnd() * 0.6 })
    return cells
  },
  serverless: (cols) => {
    // Processes on the left, each with a line straight to the bucket on the right; nothing in between.
    const cells: Cell[] = []
    const left = cols - 26
    for (const y of [1, 4, 7, 10]) {
      for (const [dx, dy] of [
        [1, 0],
        [2, 0],
        [1, 1],
        [2, 1],
      ])
        cells.push({ x: dx ?? 0, y: y + (dy ?? 0), color: "var(--muted-foreground)", alpha: 0.9 })
      for (let x = 5; x < left - 1; x += 2) cells.push({ x, y, color: "var(--muted-foreground)", alpha: 0.45 })
    }
    for (let x = left; x < cols - 1; x++)
      for (const y of [0, ROWS - 1]) cells.push({ x, y, color: "var(--primary)", alpha: 1 })
    for (let y = 0; y < ROWS; y++)
      for (const x of [left, cols - 2]) cells.push({ x, y, color: "var(--primary)", alpha: 1 })
    for (let y = 2; y < ROWS - 2; y += 3)
      for (let x = left + 3; x < cols - 4; x += 4)
        for (const dx of [0, 1]) cells.push({ x: x + dx, y, color: "var(--primary)", alpha: 0.55 })
    return cells
  },
  layout: (cols) => {
    // What queries on time series draw: a bar chart on the left and a line chart on the right, from one series.
    const rnd = seeded(7)
    const cells: Cell[] = []
    const half = Math.floor((cols - 4) / 2)
    const level = (t: number) => 0.5 + 0.25 * Math.sin(t * 6 + 1) + 0.12 * Math.sin(t * 17) + (rnd() - 0.5) * 0.1
    const height = (t: number) => Math.max(1, Math.round(level(t) * (ROWS - 2)))
    for (let x = 0; x + 1 < half; x += 3) {
      const h = height(x / half)
      for (const dx of [0, 1])
        for (let k = 0; k < h; k++) cells.push({ x: x + dx, y: ROWS - 1 - k, color: "var(--primary)", alpha: 0.85 })
    }
    let prev: number | null = null
    for (let x = half + 4; x < cols; x++) {
      const y = ROWS - 1 - height((x - half - 4) / half)
      const [top, bottom] = prev === null ? [y, y] : [Math.min(prev, y), Math.max(prev, y)]
      for (let k = bottom + 1; k < ROWS; k++) cells.push({ x, y: k, color: "var(--chart-4)", alpha: 0.18 })
      for (let k = top; k <= bottom; k++) cells.push({ x, y: k, color: "var(--chart-4)", alpha: 1 })
      prev = y
    }
    return cells
  },
  fast: (cols) => {
    // Bars as long as the query times: Timelake first, then the other lakes.
    const bars: [number, string][] = [
      [0.04, "var(--primary)"],
      [0.5, "var(--chart-5)"],
      [0.72, "var(--chart-4)"],
      [0.95, "var(--chart-3)"],
    ]
    return bars.flatMap(([share, color], k) =>
      Array.from({ length: Math.max(2, Math.round(share * cols)) }, (_, x) =>
        [0, 1].map((dy) => ({ x, y: 1 + k * 3 + dy, color, alpha: 0.9 })),
      ).flat(),
    )
  },
  cheap: (cols) => {
    // A bill over time: a server's hours fill every column; a bucket's storage is a thin line, and its requests show
    // only when something reads or writes, in green for what you save.
    const rnd = seeded(12)
    const cells: Cell[] = []
    for (let x = 0; x < cols; x++) {
      for (const y of [1, 2, 3]) cells.push({ x, y, color: "var(--muted-foreground)", alpha: 0.55 })
      cells.push({ x, y: ROWS - 2, color: "var(--chart-3)", alpha: 0.4 })
      if (rnd() < 0.15) {
        const height = 1 + Math.floor(rnd() * 4)
        for (let k = 0; k < height; k++) cells.push({ x, y: ROWS - 4 - k, color: "var(--chart-3)", alpha: 0.9 })
      }
    }
    return cells
  },
  stores: (cols) => {
    // Buckets side by side, each with a few files: one per store the card names when there is room.
    const rnd = seeded(10)
    const cells: Cell[] = []
    const count = cols >= 96 ? 6 : 4
    const width = Math.floor((cols - count + 1) / count)
    for (let b = 0; b < count; b++) {
      const left = b * (width + 1)
      const right = left + width - 2
      for (let x = left; x <= right; x++)
        for (const y of [1, ROWS - 2]) cells.push({ x, y, color: "var(--muted-foreground)", alpha: 0.7 })
      for (let y = 1; y < ROWS - 1; y++)
        for (const x of [left, right]) cells.push({ x, y, color: "var(--muted-foreground)", alpha: 0.7 })
      for (let y = 3; y < ROWS - 3; y += 2)
        for (let x = left + 2; x < right - 1; x += 2)
          if (rnd() < 0.6) cells.push({ x, y, color: "var(--primary)", alpha: 0.85 })
    }
    return cells
  },
  formats: (cols) => {
    // Iceberg metadata along the top, pointing down to rows of Parquet files.
    const cells: Cell[] = []
    for (let x = 1; x < cols - 4; x += 10) {
      for (const dx of [0, 1, 2])
        for (const y of [0, 1]) cells.push({ x: x + dx, y, color: "var(--primary)", alpha: 1 })
      for (let y = 3; y < 5; y++) cells.push({ x: x + 1, y, color: "var(--primary)", alpha: 0.45 })
    }
    for (let top = 6; top < ROWS - 1; top += 4)
      for (let x = 0; x < cols - 5; x += 7)
        for (let dx = 0; dx < 6; dx++)
          for (const dy of [0, 1, 2])
            cells.push({ x: x + dx, y: top + dy, color: "var(--muted-foreground)", alpha: dy === 0 ? 0.8 : 0.45 })
    return cells
  },
}

/**
 * A use case's picture in squares on a dim grid. It draws itself in once, left to right, when it comes into view,
 * and again when the card is hovered; otherwise it stays still.
 */
export function UseCaseArt({ kind, cols = COLS }: { kind: ArtKind; cols?: number }) {
  const canvas = useRef<HTMLCanvasElement>(null)

  useEffect(() => {
    const c = canvas.current
    const ctx = c?.getContext("2d")
    if (!c || !ctx) return
    fitCanvas(c, ctx, cols * CELL, ROWS * CELL)
    const cells = PICTURES[kind](cols).filter((p) => p.x >= 0 && p.x < cols && p.y >= 0 && p.y < ROWS)
    // How far the picture is drawn, so a theme change can redraw it as it was.
    let drawn = -1
    // The theme's colors, read once and again on a theme change: reading them every frame of the draw-in is slow.
    let colors = new Map<string, string>()
    let paper = ""
    const readColors = () => {
      colors = new Map(cells.map((p) => [p.color, resolveColor(c, p.color)]))
      paper = resolveColor(c, "var(--dot)")
    }
    readColors()

    const draw = (upTo: number) => {
      drawn = upTo
      ctx.clearRect(0, 0, cols * CELL, ROWS * CELL)
      ctx.globalAlpha = 0.6
      ctx.fillStyle = paper
      for (let x = 0; x < cols; x++) for (let y = 0; y < ROWS; y++) ctx.fillRect(x * CELL, y * CELL, DOT, DOT)
      for (const p of cells) {
        if (p.x > upTo) continue
        ctx.globalAlpha = p.alpha
        ctx.fillStyle = colors.get(p.color) ?? paper
        ctx.fillRect(p.x * CELL, p.y * CELL, DOT, DOT)
      }
      ctx.globalAlpha = 1
    }

    let frame = 0
    const play = () => {
      cancelAnimationFrame(frame)
      const start = performance.now()
      const tick = (now: number) => {
        const t = Math.min(1, (now - start) / REVEAL_MS)
        draw(t * cols)
        if (t < 1) frame = requestAnimationFrame(tick)
      }
      frame = requestAnimationFrame(tick)
    }

    draw(-1)
    const seen = new IntersectionObserver(([e]) => {
      if (e?.isIntersecting) {
        play()
        seen.disconnect()
      }
    })
    seen.observe(c)
    const card = c.closest("[data-use-case]")
    card?.addEventListener("pointerenter", play)
    const unwatch = watchTheme(() => {
      readColors()
      draw(drawn)
    })
    return () => {
      unwatch()
      cancelAnimationFrame(frame)
      seen.disconnect()
      card?.removeEventListener("pointerenter", play)
    }
  }, [kind, cols])

  // Sized in the HTML too, so the picture takes its height before the script draws it.
  return (
    <canvas
      ref={canvas}
      aria-hidden
      width={cols * CELL}
      height={ROWS * CELL}
      className="block h-auto w-full"
      style={{ maxWidth: cols * CELL }}
    />
  )
}
