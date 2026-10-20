"use client"

import { useEffect, useRef } from "react"

import { BATCH, type File, Writer, widthOf } from "@/components/hero-writer"
import { watchTheme } from "@/lib/theme"

const DOT = 3
const CELL = 4
/** Milliseconds per tick while hovered: one new point, or one step of a reader's chart. */
const TICK_MS = 55
/** How far each new point moves toward the cursor (0..1). */
const FOLLOW = 0.45
/** Rows kept clear above and below the chart, so the bounds' labels always fit. */
const PAD = 5
/** Columns of the object store, and the gaps around it. */
const STORE = 44
const GAP = 6
/** Below this many columns there is no room for readers. */
const MIN_COLS_FOR_READERS = 200
/** Ticks one reader's turn lasts when Rotate takes them in turn (about 7 s), and the ticks its chart takes to draw in. */
const CYCLE = 130
const REVEAL = 20
/** Ticks a reader takes to load a snapshot (the loading bar), and between reloads while you publish. */
const LOAD = 8
const RELOAD = 45

/** A reader, its query, the files that query reads (days are today first), and the chart it draws. */
type Reader = {
  name: string
  query: string
  pick: (days: File[][]) => File[]
  chart: "line" | "latest" | "bars" | "candles" | "peak" | "parallel"
  /** Points per bucket: the reader downsamples the series to this. */
  per: number
}
const READERS: Reader[] = [
  { name: "Mobile", query: "latest value", pick: () => [], chart: "latest", per: 1 },
  { name: "Dashboard", query: "avg per minute", pick: (d) => (d[0] ?? []).slice(-2), chart: "line", per: 8 },
  { name: "DuckDB", query: "avg per hour", pick: (d) => d[0] ?? [], chart: "bars", per: 10 },
  { name: "Trading", query: "OHLC per minute", pick: (d) => d[0] ?? [], chart: "candles", per: 8 },
  { name: "AI agent", query: "when was the peak?", pick: (d) => d[0] ?? [], chart: "peak", per: 4 },
  { name: "Many", query: "queries at once", pick: (d) => d.flat(), chart: "parallel", per: 1 },
]

/** 5×7 dot-matrix digits for the latest value, top row first. */
const DIGITS: Record<string, string[]> = {
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
}

/** One color per parallel reader, spread around the color wheel. */
const hue = (i: number, n: number) => `hsl(${Math.round((i * 360) / n + 30) % 360} 70% 62%)`
/** Columns and rows of one parallel reader's tile, gaps included. */
const TILE = [15, 8] as const

/** The series' shape when nobody is hovering: height (0..1, 0 at the top) at column x of `cols`. */
const shape = (x: number, cols: number) => 0.5 + Math.sin((x / cols) * 6 + 1) * 0.3 + Math.sin((x / cols) * 23) * 0.06
/** A reading for a height: 0 at the bottom, 100 at the top. */
const reading = (v: number) => Math.round((1 - v) * 100)

/** A bucket of points: average, highest and lowest (`top`/`bottom`, as heights with 0 at the top), first and last. */
type Bucket = { avg: number; top: number; bottom: number; open: number; close: number }

/** The series in buckets of `per` points. */
function downsample(values: number[], per: number): Bucket[] {
  const out: Bucket[] = []
  for (let i = 0; i < values.length; i += per) {
    const b = values.slice(i, i + per)
    out.push({
      avg: b.reduce((s, v) => s + v, 0) / b.length,
      top: Math.min(...b),
      bottom: Math.max(...b),
      open: b[0] ?? 0,
      close: b.at(-1) ?? 0,
    })
  }
  return out
}

/**
 * The hero as a pipeline, left to right: you publish points with the cursor; they are written to the object store
 * (batched in memory, committed as files in day partitions, compacted); and readers query the table: the files a query
 * needs light up in the reading color, and the reader draws the series downsampled its own way. Readers see the table
 * as of the last commit, so their chart lags the live one and moves a commit at a time.
 *
 * The readers are tabs on top: Rotate, the default, takes them in turn, and hovering a reader's tab keeps that one on
 * screen. Hovering the chart publishes and the reader reloads as data commits; hovering the rest publishes nothing.
 */
export function HeroFlow() {
  const canvas = useRef<HTMLCanvasElement>(null)

  useEffect(() => {
    const c = canvas.current
    const ctx = c?.getContext("2d")
    const area = c?.parentElement
    if (!c || !ctx || !area) return
    const css = getComputedStyle(c)
    const color = (name: string) => css.getPropertyValue(name).trim()
    // The theme's colors, read again on each draw so a theme change shows.
    let [paper, pen, muted, reads] = ["", "", "", ""]
    const font = getComputedStyle(document.body).getPropertyValue("--font-mono") || "monospace"

    const writer = new Writer()
    let values: number[] = []
    // What readers see: the series as of the last commit (points still in memory aren't readable yet).
    let committed: number[] = []
    let committedVersion = 0
    // What the reader shows: a snapshot of the committed data, its version, the load in progress (ticks left, and
    // whether it brings a new chart that draws in), ticks since the last load, and how far the chart has drawn in.
    let snapshot: number[] = []
    let snapshotVersion = 0
    let loading = 0
    let fresh = false
    let sinceLoad = 0
    let reveal = REVEAL
    // How fast each of many readers loads, relative to the load (at least 1, so every bar is full at the end).
    let paces: number[] = []
    let next = 0
    let cursorY: number | null = null
    // The reader on screen, and how far into its turn.
    let reader = 0
    let phase = 0
    // The reader kept on screen, or null to rotate; and where each tab is (0 is Rotate).
    let pinned: number | null = null
    let tabAreas: { x0: number; x1: number; y: number; i: number }[] = []
    // What the cursor is doing: publishing, reading, or nothing.
    let mode: "publish" | "read" | null = null
    let layout = { storeX: 0, readersX: 0, rows: 0, readers: false }

    const square = (x: number, y: number, fill: string, alpha: number) => {
      ctx.globalAlpha = alpha
      ctx.fillStyle = fill
      ctx.fillRect(x * CELL, y * CELL, DOT, DOT)
    }
    const label = (text: string, x: number, y: number, fill = muted) => {
      ctx.globalAlpha = 0.9
      ctx.fillStyle = fill
      ctx.font = `9px ${font}`
      ctx.fillText(text, x * CELL, y * CELL)
    }
    const rowOf = (v: number) => PAD + Math.round(Math.max(0, Math.min(1, v)) * (layout.rows - 1 - 2 * PAD))
    /** The files a reader's query reads, or yesterday's if today has none yet. */
    const pickFiles = (r: Reader) => {
      if (r.chart === "latest") return []
      const files = r.pick(writer.days)
      return files.length > 0 ? files : (writer.days[1] ?? [])
    }

    /** Where each of many readers draws in x..x+width, rows top..bottom: as many tiles as fit, row by row. */
    const tiles = (x: number, width: number, top: number, bottom: number) => {
      const [across, down] = [Math.floor(width / TILE[0]), Math.floor((bottom - top) / TILE[1])]
      return Array.from({ length: across * down }, (_, t) => ({
        x0: x + (t % across) * TILE[0],
        y0: top + 1 + Math.floor(t / across) * TILE[1],
      }))
    }

    /** How far tile `i` of many readers has loaded, `ticks` into the load (0..1). */
    const tileLoad = (i: number, ticks: number) => Math.min(1, (ticks / LOAD) * (paces[i % paces.length] ?? 1))
    /** Whether tile `i` has finished loading: its bar was full a tick ago (so the full bar shows before the chart). */
    const tileLoaded = (i: number) => loading > 0 && tileLoad(i, LOAD - loading) >= 1

    /** A reader's chart in x..x+width, rows top..bottom, drawn `done` (0..1) of the way, in the reading color. */
    const chart = (r: Reader, x: number, width: number, top: number, bottom: number, done: number) => {
      const buckets = downsample(snapshot, r.per)
      // Scaled to the buckets' own range, so the shape fills the chart.
      const [lo, hi] = [Math.min(...buckets.map((b) => b.top)), Math.max(...buckets.map((b) => b.bottom))]
      const at = (v: number) => top + Math.round(((v - lo) / (hi - lo || 1)) * (bottom - top))
      const upTo = Math.ceil(width * done)
      // Each bucket's columns, for the charts drawn a bucket at a time.
      const step = width / buckets.length
      const span = (i: number) => [x + Math.round(i * step), x + Math.round((i + 1) * step) - 1] as const
      const shown = buckets.filter((_, i) => span(i)[0] - x <= upTo)
      if (r.chart === "latest") {
        // The newest value in big digits (each dot 2×2 squares), and whether it rose or fell; one number, so it shows
        // at once rather than drawing in.
        const last = snapshot.at(-1) ?? 0.5
        const before = snapshot.at(-6) ?? last
        const text = String(reading(last))
        const scale = 2
        ;[...text].forEach((ch, k) =>
          (DIGITS[ch] ?? []).forEach((rowBits, ry) =>
            [...rowBits].forEach((bit, rx) => {
              if (bit !== "1") return
              for (let dx = 0; dx < scale; dx++)
                for (let dy = 0; dy < scale; dy++)
                  square(x + k * 6 * scale + rx * scale + dx, top + 4 + ry * scale + dy, reads, 1)
            }),
          ),
        )
        const ax = x + text.length * 6 * scale + 3
        const up = last < before
        ;[
          [2, 0],
          [1, 1],
          [2, 1],
          [3, 1],
          [0, 2],
          [1, 2],
          [2, 2],
          [3, 2],
          [4, 2],
        ].forEach(([dx = 0, dy = 0]) => square(ax + dx, top + (up ? 8 + dy : 12 - dy), reads, 1))
        label(up ? "rising" : "falling", ax + 7, top + 12)
        return
      }
      if (r.chart === "parallel") {
        // As many readers as fit, each in its own color, each reading its own stretch of the series. While they load,
        // each shows the new data once its own load finishes; until then, the old chart, or nothing for a new one.
        const all = tiles(x, width, top, bottom)
        const count = all.length
        label(`${count} readers`, x, top - 1)
        all.forEach(({ x0, y0 }, t) => {
          const loaded = tileLoaded(t)
          if (loading > 0 && fresh && !loaded) return
          const data = loaded ? committed : snapshot
          const [w, h] = [TILE[0] - 3, TILE[1] - 3]
          const from = (t * 7) % Math.max(1, data.length - w)
          const slice = data.slice(from, from + w)
          const [least, most] = [Math.min(...slice), Math.max(...slice)]
          slice.forEach((v, i) =>
            square(x0 + i, y0 + Math.round(((v - least) / (most - least || 1)) * h), hue(t, count), 1),
          )
        })
        return
      }
      if (r.chart === "bars") {
        shown.forEach((b, i) => {
          const [x0, x1] = span(i)
          for (let cx = x0; cx < x1; cx++) for (let cy = at(b.avg); cy <= bottom; cy++) square(cx, cy, reads, 0.8)
        })
        return
      }
      if (r.chart === "candles") {
        // A wick from high to low, and a body from open to close: solid when it rose, hollow when it fell.
        shown.forEach((b, i) => {
          const [x0, x1] = span(i)
          const mid = Math.floor((x0 + x1) / 2)
          const [left, right] = step >= 6 ? [mid - 1, mid + 2] : [mid - 1, mid + 1]
          for (let cy = at(b.top); cy <= at(b.bottom); cy++) square(mid, cy, reads, 0.7)
          const [y0, y1] = [Math.min(at(b.open), at(b.close)), Math.max(at(b.open), at(b.close))]
          const rose = b.close < b.open
          for (let cx = left; cx <= right; cx++)
            for (let cy = y0; cy <= y1; cy++) {
              const edge = cx === left || cx === right || cy === y0 || cy === y1
              if (rose || edge) square(cx, cy, reads, 1)
            }
        })
        return
      }
      if (r.chart === "peak") {
        // A step per bucket, joined to the one before; the peak's step full strength and marked.
        const peak = buckets.reduce((best, b, i) => ((buckets[best]?.avg ?? 1) > b.avg ? i : best), 0)
        let prev: number | null = null
        shown.forEach((b, i) => {
          const [x0, x1] = span(i)
          const y = at(b.avg)
          if (prev !== null) for (let cy = Math.min(prev, y); cy <= Math.max(prev, y); cy++) square(x0, cy, reads, 0.6)
          for (let cx = x0; cx <= x1; cx++) square(cx, y, reads, i === peak ? 1 : 0.6)
          prev = y
        })
        const b = buckets[peak]
        if (b && done >= 1) {
          const [x0, y] = [span(peak)[0], at(b.avg)]
          for (const [dx, dy] of [
            [-1, -2],
            [0, -3],
            [1, -2],
          ] as const)
            square(x0 + dx, y + dy, reads, 1)
          label(`peak ${reading(b.avg)}`, Math.min(x0 + 6, x + width - 10), Math.max(top + 2, y - 2), reads)
        }
        return
      }
      // The series downsampled: a point per bucket, mid-bucket, joined to the next by a faint line.
      const points = shown.map((b, i) => [Math.round((span(i)[0] + span(i)[1]) / 2), at(b.avg)] as const)
      points.forEach(([px, py], i) => {
        const [nx, ny] = points[i + 1] ?? [px, py]
        let prev = py
        for (let cx = px; cx <= nx; cx++) {
          const y = Math.round(py + ((ny - py) * (cx - px)) / (nx - px || 1))
          for (let cy = Math.min(prev, y); cy <= Math.max(prev, y); cy++) square(cx, cy, reads, 0.35)
          prev = y
        }
        for (const [dx, dy] of [
          [0, 0],
          [1, 0],
          [0, 1],
          [1, 1],
        ] as const)
          square(px + dx, py + dy, reads, 1)
      })
    }

    const draw = () => {
      ;[paper, pen, muted, reads] = [
        color("--border"),
        color("--primary"),
        color("--muted-foreground"),
        color("--chart-4"),
      ]
      const dpr = window.devicePixelRatio || 1
      const [w, h] = [c.clientWidth, c.clientHeight]
      if (c.width !== Math.round(w * dpr) || c.height !== Math.round(h * dpr)) {
        c.width = Math.round(w * dpr)
        c.height = Math.round(h * dpr)
      }
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
      ctx.clearRect(0, 0, w, h)
      const cols = Math.floor(w / CELL)
      const rows = Math.floor(h / CELL)
      const readers = cols >= MIN_COLS_FOR_READERS
      const chartEnd = Math.floor(cols * (readers ? 0.4 : 0.5))
      const storeX = chartEnd + GAP
      const readersX = storeX + STORE + GAP
      layout = { storeX, readersX, rows, readers }
      tabAreas = []
      const chartCols = chartEnd - 1
      if (values.length !== chartCols) {
        const kept = values.slice(-chartCols)
        next = chartCols
        values = [...Array.from({ length: chartCols - kept.length }, (_, x) => shape(x, chartCols)), ...kept]
        committed = values.slice(0, values.length - writer.buffered)
        committedVersion = writer.version
        if (snapshot.length === 0) [snapshot, snapshotVersion] = [committed, committedVersion]
      }

      for (let x = 0; x < cols; x++) for (let y = 0; y < rows; y++) square(x, y, paper, 0.55)
      for (const edge of readers ? [storeX - 3, readersX - 3] : [storeX - 3])
        for (let y = 0; y < rows; y += 2) square(edge, y, muted, 0.35)

      // The series you publish, and its bounds.
      const [hi, lo] = [Math.min(...values), Math.max(...values)]
      for (const [v, name] of [
        [hi, "max"],
        [lo, "min"],
      ] as const) {
        const y = rowOf(v)
        for (let x = 1; x < chartEnd; x += 2) square(x, y, muted, 0.6)
        label(`${name} ${reading(v)}`, chartEnd - 12, name === "max" ? y - 1 : y + 4)
      }
      let prev: number | null = null
      values.forEach((v, i) => {
        const y = rowOf(v)
        const [a, b] = prev === null ? [y, y] : [Math.min(prev, y), Math.max(prev, y)]
        for (let r = a; r <= b; r++) square(1 + i, r, pen, 1)
        prev = y
      })

      // The object store: path, memory, version, then the partitions; the files being read in the reading color.
      const turn = readers ? READERS[reader] : undefined
      const read = new Set(turn ? pickFiles(turn) : [])
      label("s3://lakehouse/data", storeX, 4, pen)
      label(`memory ${writer.buffered}/${BATCH}`, storeX, 8)
      for (let i = 0; i < BATCH; i++)
        for (const [dx, dy] of [
          [0, 1],
          [1, 1],
          [0, 2],
          [1, 2],
        ] as const)
          square(storeX + i * 3 + dx, 8 + dy, pen, i < writer.buffered ? 1 : 0.15)
      label(`v${writer.version} ${writer.event}`, storeX, 15, pen)
      // The last-value index beside memory, which the mobile app reads.
      const index = turn?.chart === "latest"
      label("index", storeX + 30, 8, index ? reads : muted)
      for (let dx = 0; dx < 6; dx++)
        for (const dy of [1, 2]) square(storeX + 30 + dx, 8 + dy, index ? reads : pen, index ? 1 : 0.35)
      writer.days.forEach((list, d) => {
        const y = 20 + d * 8
        label(writer.partition(d), storeX, y)
        let x = storeX
        for (const f of list) {
          const width = widthOf(f)
          if (x + width > storeX + STORE) break
          // Many readers color each file with one of theirs; any other reader in the reading color.
          const many = turn?.chart === "parallel"
          const n = writer.days.flat().indexOf(f)
          const [fill, alpha] = read.has(f)
            ? [many ? hue(n * 5, 24) : reads, 1]
            : [pen, f === writer.fresh ? 0.9 : 0.35]
          for (let dx = 0; dx < width; dx++) for (const dy of [1, 2, 3]) square(x + dx, y + dy, fill, alpha)
          x += width + 1
        }
      })

      // The reader: who, what it asked, what it read, and its chart.
      if (turn) {
        const done = Math.min(1, reveal / REVEAL)
        // While a new chart loads there is nothing to show yet; a reload keeps the old one up.
        const show = !(loading > 0 && fresh)
        const files =
          turn.chart === "latest"
            ? `as of v${snapshotVersion} · reads the last-value index, 0 files`
            : `as of v${snapshotVersion} · reads ${read.size} of ${writer.days.flat().length} files`
        const bar = (x0: number, width: number, y: number) => {
          if (loading === 0) return
          // Full on the last tick of the load, so the bar finishes before the chart shows.
          const filled = Math.round((width * (LOAD - loading + 1)) / LOAD)
          for (let i = 0; i < width; i++) square(x0 + i, y, reads, i < filled ? 1 : 0.15)
        }
        /** The loading bar at row `barY` (many readers get one each, over their tiles), then the chart from `top`. */
        const body = (x0: number, width: number, barY: number, top: number) => {
          if (loading > 0 && turn.chart === "parallel") {
            const all = tiles(x0, width, top, rows - 7)
            all.forEach((t, i) => {
              if (tileLoaded(i)) return
              const filled = Math.round((TILE[0] - 3) * tileLoad(i, LOAD - loading + 1))
              for (let dx = 0; dx < TILE[0] - 3; dx++)
                square(t.x0 + dx, t.y0 - 1, hue(i, all.length), dx < filled ? 1 : 0.15)
            })
          } else bar(x0, width, barY)
          // Many readers decide tile by tile what to show while loading.
          if (show || turn.chart === "parallel") chart(turn, x0, width, top, rows - 7, done)
        }
        // Rotate, then a tab per reader, wrapping to another row when they don't fit. The selected tab is
        // underlined; the reader on screen is in the reading color.
        ctx.font = `9px ${font}`
        let [tx, ty] = [readersX, 4]
        ;["Rotate", ...READERS.map((r) => r.name)].forEach((name, i) => {
          const span = Math.ceil(ctx.measureText(name).width / CELL)
          if (tx + span > cols - 2 && tx > readersX) [tx, ty] = [readersX, ty + 5]
          const selected = i === 0 ? pinned === null : pinned === i - 1
          label(name, tx, ty, selected || i - 1 === reader ? reads : muted)
          if (selected) for (let dx = 0; dx < span; dx++) square(tx + dx, ty + 2, reads, 0.6)
          tabAreas.push({ x0: tx, x1: tx + span, y: ty, i })
          tx += span + 3
        })
        label(`${turn.query} · ${files}`, readersX, rows - 2)
        body(readersX, cols - readersX - 2, ty + 5, ty + 8)
      }
      ctx.globalAlpha = 1
    }

    /** Starts loading a snapshot; `isFresh` when it is for another reader, whose chart then draws in. */
    const load = (isFresh: boolean) => {
      loading = LOAD
      fresh = isFresh
      paces = Array.from({ length: 64 }, () => 1 + Math.random())
    }

    /** One tick: publish a point (it is written), and move the readers' loads and charts along. */
    const tick = () => {
      if (mode === "publish") {
        const last = values.at(-1) ?? 0.5
        const value = cursorY === null ? shape(next, values.length) : last + (cursorY - last) * FOLLOW
        values = [...values.slice(1), value]
        next++
        if (writer.write()) [committed, committedVersion] = [[...values], writer.version]
      }
      // While hovered, readers take turns (unless a tab keeps one on screen), and the one on screen reloads while you
      // publish.
      if (mode !== null && pinned === null && ++phase > CYCLE) {
        phase = 0
        reader = (reader + 1) % READERS.length
        load(true)
      } else if (mode === "publish" && ++sinceLoad >= RELOAD && loading === 0) load(false)
      if (loading > 0 && --loading === 0) {
        ;[snapshot, snapshotVersion, sinceLoad] = [[...committed], committedVersion, 0]
        if (fresh) reveal = 0
      } else if (loading === 0 && reveal < REVEAL) reveal++
      draw()
      // When not publishing or rotating (or no longer hovered), nothing needs ticks once the chart is loaded and drawn,
      // until the cursor does something; a load or draw-in under way always finishes first.
      const idle = mode === null || (pinned !== null && mode !== "publish")
      if (idle && loading === 0 && reveal >= REVEAL) stop()
    }

    let timer: ReturnType<typeof setInterval> | null = null
    const run = () => {
      if (!timer) timer = setInterval(tick, TICK_MS)
    }
    const stop = () => {
      if (timer) clearInterval(timer)
      timer = null
    }

    /** Selects tab `i`: Rotate (0) carries on from the reader on screen; a reader's tab keeps it on screen. */
    const select = (i: number) => {
      if (i === 0) {
        if (pinned !== null) [pinned, phase] = [null, 0]
        return
      }
      pinned = i - 1
      if (reader !== pinned) {
        reader = pinned
        load(true)
      }
    }

    draw()
    // With reduced motion the hero stays a still picture: nothing follows the pointer.
    const still = window.matchMedia("(prefers-reduced-motion: reduce)").matches
    const move = (e: PointerEvent) => {
      const r = c.getBoundingClientRect()
      const [x, y] = [(e.clientX - r.left) / CELL, (e.clientY - r.top) / CELL]
      const height = Math.floor(r.height / CELL)
      // Publishing over the chart; elsewhere no new data, and a tab under the cursor is selected.
      const onChart = x < layout.storeX - 3
      cursorY = onChart ? Math.max(0, Math.min(1, (y - PAD) / (height - 1 - 2 * PAD))) : null
      mode = onChart ? "publish" : "read"
      const tab = tabAreas.find((t) => x >= t.x0 - 1 && x <= t.x1 + 1 && y >= t.y - 4 && y <= t.y + 3)
      if (tab) select(tab.i)
      run()
    }
    // The reader reloads as soon as the cursor comes in, so the readers visibly respond to hovering.
    const enter = () => {
      if (!layout.readers || loading > 0) return
      load(true)
      run()
    }
    // Leaving stops publishing and rotating; the next tick stops the hero once the reader has finished loading and
    // drawing in.
    const leave = () => {
      cursorY = null
      mode = null
    }
    if (!still) {
      area.addEventListener("pointerenter", enter)
      area.addEventListener("pointermove", move)
      area.addEventListener("pointerleave", leave)
    }
    const ro = new ResizeObserver(draw)
    ro.observe(c)
    const unwatch = watchTheme(draw)
    return () => {
      stop()
      unwatch()
      area.removeEventListener("pointerenter", enter)
      area.removeEventListener("pointermove", move)
      area.removeEventListener("pointerleave", leave)
      ro.disconnect()
    }
  }, [])

  return <canvas ref={canvas} aria-hidden className="absolute inset-0 h-full w-full" />
}
