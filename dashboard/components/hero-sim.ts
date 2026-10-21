/**
 * The hero's picture as a state machine, apart from the page: the series you publish, what the writer has committed,
 * the reader on screen and what it has loaded, and how all of that is drawn in squares. components/hero-flow.tsx
 * wires it to a canvas, a timer and the pointer.
 */
import { BATCH, type File, Writer, widthOf } from "@/components/hero-writer"
import { CELL, DOT, DOT_FONT } from "@/lib/dot-canvas"

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

/** One color per parallel reader, spread around the color wheel; `tone` is the theme's saturation and lightness. */
const hue = (i: number, n: number, tone: string) => `hsl(${Math.round((i * 360) / n + 30) % 360} ${tone})`
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

/** The theme's colors the hero draws with, and the font of its labels. */
export type HeroTheme = { paper: string; pen: string; muted: string; reads: string; tone: string; font: string }

/** What a tick leaves the hero doing: still running, or idle until the cursor does something. */
export type TickResult = "running" | "idle"

export class HeroSim {
  private writer = new Writer()
  private values: number[] = []
  // What readers see: the series as of the last commit (points still in memory aren't readable yet).
  private committed: number[] = []
  private committedVersion = 0
  // What the reader shows: a snapshot of the committed data, its version, the load in progress (ticks left, and
  // whether it brings a new chart that draws in), ticks since the last load, and how far the chart has drawn in.
  private snapshot: number[] = []
  private snapshotVersion = 0
  private loading = 0
  private fresh = false
  private sinceLoad = 0
  private reveal = REVEAL
  // How fast each of many readers loads, relative to the load (at least 1, so every bar is full at the end).
  private paces: number[] = []
  private next = 0
  private cursorY: number | null = null
  // The reader on screen, and how far into its turn.
  private reader = 0
  private phase = 0
  // The reader kept on screen, or null to rotate; and where each tab is (0 is Rotate).
  private pinned: number | null = null
  private tabAreas: { x0: number; x1: number; y: number; i: number }[] = []
  // What the cursor is doing: publishing, reading, or nothing.
  private mode: "publish" | "read" | null = null
  private layout = { storeX: 0, readersX: 0, rows: 0, readers: false }
  // Set for the length of a draw.
  private ctx: CanvasRenderingContext2D | null = null
  private theme: HeroTheme = { paper: "", pen: "", muted: "", reads: "", tone: "", font: "monospace" }

  private square(x: number, y: number, fill: string, alpha: number) {
    if (!this.ctx) return
    this.ctx.globalAlpha = alpha
    this.ctx.fillStyle = fill
    this.ctx.fillRect(x * CELL, y * CELL, DOT, DOT)
  }

  private label(text: string, x: number, y: number, fill = this.theme.muted) {
    if (!this.ctx) return
    this.ctx.globalAlpha = 0.9
    this.ctx.fillStyle = fill
    this.ctx.font = `9px ${this.theme.font}`
    this.ctx.fillText(text, x * CELL, y * CELL)
  }

  private rowOf(v: number) {
    return PAD + Math.round(Math.max(0, Math.min(1, v)) * (this.layout.rows - 1 - 2 * PAD))
  }

  /** The files a reader's query reads, or yesterday's if today has none yet. */
  private pickFiles(r: Reader) {
    if (r.chart === "latest") return []
    const files = r.pick(this.writer.days)
    return files.length > 0 ? files : (this.writer.days[1] ?? [])
  }

  /** Where each of many readers draws in x..x+width, rows top..bottom: as many tiles as fit, row by row. */
  private tiles(x: number, width: number, top: number, bottom: number) {
    const [across, down] = [Math.floor(width / TILE[0]), Math.floor((bottom - top) / TILE[1])]
    return Array.from({ length: across * down }, (_, t) => ({
      x0: x + (t % across) * TILE[0],
      y0: top + 1 + Math.floor(t / across) * TILE[1],
    }))
  }

  /** How far tile `i` of many readers has loaded, `ticks` into the load (0..1). */
  private tileLoad(i: number, ticks: number) {
    return Math.min(1, (ticks / LOAD) * (this.paces[i % this.paces.length] ?? 1))
  }

  /** Whether tile `i` has finished loading: its bar was full a tick ago (so the full bar shows before the chart). */
  private tileLoaded(i: number) {
    return this.loading > 0 && this.tileLoad(i, LOAD - this.loading) >= 1
  }

  /** A reader's chart in x..x+width, rows top..bottom, drawn `done` (0..1) of the way, in the reading color. */
  private chart(r: Reader, x: number, width: number, top: number, bottom: number, done: number) {
    const { reads, tone } = this.theme
    const snapshot = this.snapshot
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
        (DOT_FONT[ch] ?? []).forEach((rowBits, ry) =>
          [...rowBits].forEach((bit, rx) => {
            if (bit !== "1") return
            for (let dx = 0; dx < scale; dx++)
              for (let dy = 0; dy < scale; dy++)
                this.square(x + k * 6 * scale + rx * scale + dx, top + 4 + ry * scale + dy, reads, 1)
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
      ].forEach(([dx = 0, dy = 0]) => this.square(ax + dx, top + (up ? 8 + dy : 12 - dy), reads, 1))
      this.label(up ? "rising" : "falling", ax + 7, top + 12)
      return
    }
    if (r.chart === "parallel") {
      // As many readers as fit, each in its own color, each reading its own stretch of the series. While they load,
      // each shows the new data once its own load finishes; until then, the old chart, or nothing for a new one.
      const all = this.tiles(x, width, top, bottom)
      const count = all.length
      this.label(`${count} readers`, x, top - 1)
      all.forEach(({ x0, y0 }, t) => {
        const loaded = this.tileLoaded(t)
        if (this.loading > 0 && this.fresh && !loaded) return
        const data = loaded ? this.committed : snapshot
        const [w, h] = [TILE[0] - 3, TILE[1] - 3]
        const from = (t * 7) % Math.max(1, data.length - w)
        const slice = data.slice(from, from + w)
        const [least, most] = [Math.min(...slice), Math.max(...slice)]
        slice.forEach((v, i) =>
          this.square(x0 + i, y0 + Math.round(((v - least) / (most - least || 1)) * h), hue(t, count, tone), 1),
        )
      })
      return
    }
    if (r.chart === "bars") {
      shown.forEach((b, i) => {
        const [x0, x1] = span(i)
        for (let cx = x0; cx < x1; cx++) for (let cy = at(b.avg); cy <= bottom; cy++) this.square(cx, cy, reads, 0.8)
      })
      return
    }
    if (r.chart === "candles") {
      // A wick from high to low, and a body from open to close: solid when it rose, hollow when it fell.
      shown.forEach((b, i) => {
        const [x0, x1] = span(i)
        const mid = Math.floor((x0 + x1) / 2)
        const [left, right] = step >= 6 ? [mid - 1, mid + 2] : [mid - 1, mid + 1]
        for (let cy = at(b.top); cy <= at(b.bottom); cy++) this.square(mid, cy, reads, 0.7)
        const [y0, y1] = [Math.min(at(b.open), at(b.close)), Math.max(at(b.open), at(b.close))]
        const rose = b.close < b.open
        for (let cx = left; cx <= right; cx++)
          for (let cy = y0; cy <= y1; cy++) {
            const edge = cx === left || cx === right || cy === y0 || cy === y1
            if (rose || edge) this.square(cx, cy, reads, 1)
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
        if (prev !== null)
          for (let cy = Math.min(prev, y); cy <= Math.max(prev, y); cy++) this.square(x0, cy, reads, 0.6)
        for (let cx = x0; cx <= x1; cx++) this.square(cx, y, reads, i === peak ? 1 : 0.6)
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
          this.square(x0 + dx, y + dy, reads, 1)
        this.label(`peak ${reading(b.avg)}`, Math.min(x0 + 6, x + width - 10), Math.max(top + 2, y - 2), reads)
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
        for (let cy = Math.min(prev, y); cy <= Math.max(prev, y); cy++) this.square(cx, cy, reads, 0.35)
        prev = y
      }
      for (const [dx, dy] of [
        [0, 0],
        [1, 0],
        [0, 1],
        [1, 1],
      ] as const)
        this.square(px + dx, py + dy, reads, 1)
    })
  }

  /** Draws the hero into `ctx`, `w`×`h` CSS pixels, already cleared, in the theme's colors. */
  draw(ctx: CanvasRenderingContext2D, w: number, h: number, theme: HeroTheme) {
    this.ctx = ctx
    this.theme = theme
    const { paper, pen, muted, reads, tone, font } = theme
    const writer = this.writer
    const cols = Math.floor(w / CELL)
    const rows = Math.floor(h / CELL)
    const readers = cols >= MIN_COLS_FOR_READERS
    const chartEnd = Math.floor(cols * (readers ? 0.4 : 0.5))
    const storeX = chartEnd + GAP
    const readersX = storeX + STORE + GAP
    this.layout = { storeX, readersX, rows, readers }
    this.tabAreas = []
    const chartCols = chartEnd - 1
    if (this.values.length !== chartCols) {
      const kept = this.values.slice(-chartCols)
      this.next = chartCols
      this.values = [...Array.from({ length: chartCols - kept.length }, (_, x) => shape(x, chartCols)), ...kept]
      this.committed = this.values.slice(0, this.values.length - writer.buffered)
      this.committedVersion = writer.version
      if (this.snapshot.length === 0) [this.snapshot, this.snapshotVersion] = [this.committed, this.committedVersion]
    }

    for (let x = 0; x < cols; x++) for (let y = 0; y < rows; y++) this.square(x, y, paper, 0.55)
    for (const edge of readers ? [storeX - 3, readersX - 3] : [storeX - 3])
      for (let y = 0; y < rows; y += 2) this.square(edge, y, muted, 0.35)

    // The series you publish, and its bounds.
    const [hi, lo] = [Math.min(...this.values), Math.max(...this.values)]
    for (const [v, name] of [
      [hi, "max"],
      [lo, "min"],
    ] as const) {
      const y = this.rowOf(v)
      for (let x = 1; x < chartEnd; x += 2) this.square(x, y, muted, 0.6)
      this.label(`${name} ${reading(v)}`, chartEnd - 12, name === "max" ? y - 1 : y + 4)
    }
    let prev: number | null = null
    this.values.forEach((v, i) => {
      const y = this.rowOf(v)
      const [a, b] = prev === null ? [y, y] : [Math.min(prev, y), Math.max(prev, y)]
      for (let r = a; r <= b; r++) this.square(1 + i, r, pen, 1)
      prev = y
    })

    // The object store: path, memory, version, then the partitions; the files being read in the reading color.
    const turn = readers ? READERS[this.reader] : undefined
    const read = new Set(turn ? this.pickFiles(turn) : [])
    this.label("s3://lakehouse/data", storeX, 4, pen)
    this.label(`memory ${writer.buffered}/${BATCH}`, storeX, 8)
    for (let i = 0; i < BATCH; i++)
      for (const [dx, dy] of [
        [0, 1],
        [1, 1],
        [0, 2],
        [1, 2],
      ] as const)
        this.square(storeX + i * 3 + dx, 8 + dy, pen, i < writer.buffered ? 1 : 0.15)
    this.label(`v${writer.version} ${writer.event}`, storeX, 15, pen)
    // The last-value index beside memory, which the mobile app reads.
    const index = turn?.chart === "latest"
    this.label("index", storeX + 30, 8, index ? reads : muted)
    for (let dx = 0; dx < 6; dx++)
      for (const dy of [1, 2]) this.square(storeX + 30 + dx, 8 + dy, index ? reads : pen, index ? 1 : 0.35)
    writer.days.forEach((list, d) => {
      const y = 20 + d * 8
      this.label(writer.partition(d), storeX, y)
      let x = storeX
      for (const f of list) {
        const width = widthOf(f)
        if (x + width > storeX + STORE) break
        // Many readers color each file with one of theirs; any other reader in the reading color.
        const many = turn?.chart === "parallel"
        const n = writer.days.flat().indexOf(f)
        const [fill, alpha] = read.has(f)
          ? [many ? hue(n * 5, 24, tone) : reads, 1]
          : [pen, f === writer.fresh ? 0.9 : 0.35]
        for (let dx = 0; dx < width; dx++) for (const dy of [1, 2, 3]) this.square(x + dx, y + dy, fill, alpha)
        x += width + 1
      }
    })

    // The reader: who, what it asked, what it read, and its chart.
    if (turn) {
      const done = Math.min(1, this.reveal / REVEAL)
      // While a new chart loads there is nothing to show yet; a reload keeps the old one up.
      const show = !(this.loading > 0 && this.fresh)
      const files =
        turn.chart === "latest"
          ? `as of v${this.snapshotVersion} · reads the last-value index, 0 files`
          : `as of v${this.snapshotVersion} · reads ${read.size} of ${writer.days.flat().length} files`
      const bar = (x0: number, width: number, y: number) => {
        if (this.loading === 0) return
        // Full on the last tick of the load, so the bar finishes before the chart shows.
        const filled = Math.round((width * (LOAD - this.loading + 1)) / LOAD)
        for (let i = 0; i < width; i++) this.square(x0 + i, y, reads, i < filled ? 1 : 0.15)
      }
      /** The loading bar at row `barY` (many readers get one each, over their tiles), then the chart from `top`. */
      const body = (x0: number, width: number, barY: number, top: number) => {
        if (this.loading > 0 && turn.chart === "parallel") {
          const all = this.tiles(x0, width, top, rows - 7)
          all.forEach((t, i) => {
            if (this.tileLoaded(i)) return
            const filled = Math.round((TILE[0] - 3) * this.tileLoad(i, LOAD - this.loading + 1))
            for (let dx = 0; dx < TILE[0] - 3; dx++)
              this.square(t.x0 + dx, t.y0 - 1, hue(i, all.length, tone), dx < filled ? 1 : 0.15)
          })
        } else bar(x0, width, barY)
        // Many readers decide tile by tile what to show while loading.
        if (show || turn.chart === "parallel") this.chart(turn, x0, width, top, rows - 7, done)
      }
      // Rotate, then a tab per reader, wrapping to another row when they don't fit. The selected tab is
      // underlined; the reader on screen is in the reading color.
      ctx.font = `9px ${font}`
      let [tx, ty] = [readersX, 4]
      ;["Rotate", ...READERS.map((r) => r.name)].forEach((name, i) => {
        const span = Math.ceil(ctx.measureText(name).width / CELL)
        if (tx + span > cols - 2 && tx > readersX) [tx, ty] = [readersX, ty + 5]
        const selected = i === 0 ? this.pinned === null : this.pinned === i - 1
        this.label(name, tx, ty, selected || i - 1 === this.reader ? reads : muted)
        if (selected) for (let dx = 0; dx < span; dx++) this.square(tx + dx, ty + 2, reads, 0.6)
        this.tabAreas.push({ x0: tx, x1: tx + span, y: ty, i })
        tx += span + 3
      })
      this.label(`${turn.query} · ${files}`, readersX, rows - 2)
      body(readersX, cols - readersX - 2, ty + 5, ty + 8)
    }
    ctx.globalAlpha = 1
    this.ctx = null
  }

  /** Starts loading a snapshot; `isFresh` when it is for another reader, whose chart then draws in. */
  private load(isFresh: boolean) {
    this.loading = LOAD
    this.fresh = isFresh
    this.paces = Array.from({ length: 64 }, () => 1 + Math.random())
  }

  /**
   * One tick: publish a point (it is written), and move the readers' loads and charts along. Idle once nothing needs
   * ticks: not publishing or rotating (or no longer hovered), and the chart loaded and drawn in.
   */
  tick(): TickResult {
    if (this.mode === "publish") {
      const last = this.values.at(-1) ?? 0.5
      const value = this.cursorY === null ? shape(this.next, this.values.length) : last + (this.cursorY - last) * FOLLOW
      this.values = [...this.values.slice(1), value]
      this.next++
      if (this.writer.write()) [this.committed, this.committedVersion] = [[...this.values], this.writer.version]
    }
    // While hovered, readers take turns (unless a tab keeps one on screen), and the one on screen reloads while you
    // publish.
    if (this.mode !== null && this.pinned === null && ++this.phase > CYCLE) {
      this.phase = 0
      this.reader = (this.reader + 1) % READERS.length
      this.load(true)
    } else if (this.mode === "publish" && ++this.sinceLoad >= RELOAD && this.loading === 0) this.load(false)
    if (this.loading > 0 && --this.loading === 0) {
      ;[this.snapshot, this.snapshotVersion, this.sinceLoad] = [[...this.committed], this.committedVersion, 0]
      if (this.fresh) this.reveal = 0
    } else if (this.loading === 0 && this.reveal < REVEAL) this.reveal++
    // A load or draw-in under way always finishes first.
    const idle = this.mode === null || (this.pinned !== null && this.mode !== "publish")
    return idle && this.loading === 0 && this.reveal >= REVEAL ? "idle" : "running"
  }

  /** Selects tab `i`: Rotate (0) carries on from the reader on screen; a reader's tab keeps it on screen. */
  private select(i: number) {
    if (i === 0) {
      if (this.pinned !== null) [this.pinned, this.phase] = [null, 0]
      return
    }
    this.pinned = i - 1
    if (this.reader !== this.pinned) {
      this.reader = this.pinned
      this.load(true)
    }
  }

  /**
   * The pointer at `x`, `y` (in cells) over a hero `height` cells tall: over the chart it publishes; elsewhere no new
   * data, and a tab under it is selected.
   */
  move(x: number, y: number, height: number) {
    const onChart = x < this.layout.storeX - 3
    this.cursorY = onChart ? Math.max(0, Math.min(1, (y - PAD) / (height - 1 - 2 * PAD))) : null
    this.mode = onChart ? "publish" : "read"
    const tab = this.tabAreas.find((t) => x >= t.x0 - 1 && x <= t.x1 + 1 && y >= t.y - 4 && y <= t.y + 3)
    if (tab) this.select(tab.i)
  }

  /** The pointer came in: the reader reloads, so the readers visibly respond. Returns whether it started a load. */
  enter(): boolean {
    if (!this.layout.readers || this.loading > 0) return false
    this.load(true)
    return true
  }

  /** The pointer left: no more publishing or rotating; the hero goes idle once the reader has finished. */
  leave() {
    this.cursorY = null
    this.mode = null
  }
}
