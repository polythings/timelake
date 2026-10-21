"use client"

import { useEffect, useRef, useState } from "react"

import { fitCanvas } from "@/lib/dot-canvas"
import { formatNumber } from "@/lib/format"

type Line = { kind: "user" | "agent" | "cmd" | "out"; text: string }

/** An agent answering a question with the CLI. The commands ran on the lab's demo table; the output is theirs. */
const TRANSCRIPT: Line[] = [
  { kind: "user", text: "Which pump ran hottest yesterday, and when?" },
  { kind: "agent", text: "Checking the newest readings first." },
  { kind: "cmd", text: "timelake latest demo.readings" },
  {
    kind: "out",
    text: `+---------------------+-----------+-------+
| time                | sensor_id | temp  |
+---------------------+-----------+-------+
| 2026-10-15T23:59:00 | pump-1    | 60.34 |
| 2026-10-15T23:59:00 | pump-2    | 60.46 |
| 2026-10-15T23:59:00 | pump-3    | 69.31 |
+---------------------+-----------+-------+`,
  },
  {
    kind: "cmd",
    text: `timelake sql "SELECT sensor_id, round(max(temp), 2) AS peak FROM demo.readings WHERE time >= TIMESTAMP '2026-10-15' GROUP BY 1 ORDER BY peak DESC"`,
  },
  {
    kind: "out",
    text: `+-----------+-------+
| sensor_id | peak  |
+-----------+-------+
| pump-3    | 70.85 |
| pump-1    | 65.24 |
| pump-2    | 61.37 |
+-----------+-------+`,
  },
  {
    kind: "cmd",
    text: `timelake sql "SELECT time, temp FROM demo.readings WHERE sensor_id = 'pump-3' AND time >= TIMESTAMP '2026-10-15' ORDER BY temp DESC LIMIT 1"`,
  },
  {
    kind: "out",
    text: `+---------------------+-------+
| time                | temp  |
+---------------------+-------+
| 2026-10-15T22:03:00 | 70.85 |
+---------------------+-------+`,
  },
  { kind: "agent", text: "pump-3 ran hottest: it peaked at 70.85 at 22:03 UTC on October 15." },
]

const TYPE_MS = 12
const LINE_MS = 700
const AGENTS = 2000

/** The transcript, revealed a line at a time with commands typed out; loops. */
function Transcript() {
  const [line, setLine] = useState(0)
  const [typed, setTyped] = useState(0)

  useEffect(() => {
    const cur = TRANSCRIPT[line]
    const typing = cur?.kind === "cmd" && typed < cur.text.length
    const id = typing
      ? setTimeout(() => setTyped((t) => t + 3), TYPE_MS)
      : setTimeout(
          () => {
            setLine((l) => (l + 1 > TRANSCRIPT.length ? 0 : l + 1))
            setTyped(0)
          },
          line >= TRANSCRIPT.length ? 4000 : LINE_MS,
        )
    return () => clearTimeout(id)
  }, [line, typed])

  return (
    <pre className="h-[440px] overflow-hidden p-5 font-mono text-[11.5px] leading-[1.6] whitespace-pre-wrap">
      {TRANSCRIPT.slice(0, line + 1).map((l, i) => {
        const text = i === line && l.kind === "cmd" ? l.text.slice(0, typed) : l.text
        if (l.kind === "user")
          return (
            <span key={i} className="mb-2 block text-foreground">
              <span className="text-muted-foreground">user › </span>
              {text}
            </span>
          )
        if (l.kind === "agent")
          return (
            <span key={i} className="my-2 block text-foreground">
              <span className="text-primary">agent › </span>
              {text}
            </span>
          )
        if (l.kind === "cmd")
          return (
            <span key={i} className="block text-foreground/90">
              <span className="text-primary select-none">$ </span>
              {text}
            </span>
          )
        return (
          <span key={i} className="mb-1 block text-muted-foreground">
            {text}
          </span>
        )
      })}
    </pre>
  )
}

/** Many agents, each a dot that lights up when it reads the bucket. Illustrative. */
function Agents() {
  const canvas = useRef<HTMLCanvasElement>(null)

  useEffect(() => {
    const c = canvas.current
    const ctx = c?.getContext("2d")
    if (!c || !ctx) return
    const width = c.clientWidth
    const size = 4
    const gap = 3
    const perRow = Math.max(1, Math.floor(width / (size + gap)))
    const rows = Math.ceil(AGENTS / perRow)
    c.style.height = `${rows * (size + gap)}px`
    fitCanvas(c, ctx, width, rows * (size + gap))
    const css = getComputedStyle(c)
    const glow = new Float32Array(AGENTS)
    let frame = 0
    const tick = () => {
      // The theme's colors, read each frame so a theme change shows.
      const on = css.getPropertyValue("--primary").trim()
      const off = css.getPropertyValue("--border").trim()
      for (let n = 0; n < 40; n++) glow[Math.floor(Math.random() * AGENTS)] = 1
      for (let i = 0; i < AGENTS; i++) {
        const g = glow[i] ?? 0
        ctx.globalAlpha = 1
        ctx.fillStyle = off
        ctx.fillRect((i % perRow) * (size + gap), Math.floor(i / perRow) * (size + gap), size, size)
        if (g > 0.02) {
          ctx.globalAlpha = g
          ctx.fillStyle = on
          ctx.fillRect((i % perRow) * (size + gap), Math.floor(i / perRow) * (size + gap), size, size)
          glow[i] = g * 0.9
        }
      }
      frame = requestAnimationFrame(tick)
    }
    frame = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(frame)
  }, [])

  return <canvas ref={canvas} className="w-full" />
}

export function AgentTerminal() {
  return (
    <div className="cells lg:grid-cols-[minmax(0,1.45fr)_minmax(0,1fr)]">
      <div className="flex min-w-0 flex-col">
        <div className="flex items-center gap-1.5 border-b px-4 py-2.5">
          <span className="size-2 rounded-full bg-border" />
          <span className="size-2 rounded-full bg-border" />
          <span className="size-2 rounded-full bg-border" />
          <span className="ml-3 font-mono text-[11px] text-muted-foreground">agent · timelake CLI</span>
        </div>
        <Transcript />
      </div>
      <div className="flex flex-col gap-4 p-5">
        <div className="flex items-baseline justify-between gap-3">
          <span className="label-mono">{formatNumber(AGENTS)} agents</span>
          <span className="font-mono text-[11px] text-muted-foreground">database load: no database</span>
        </div>
        <Agents />
        <p className="mt-auto text-sm text-muted-foreground">
          Each agent runs its own reader against the bucket. There is no database whose CPU, memory, or connections they
          compete for: object storage serves the reads, and S3 handles at least 5,500 reads per second for each prefix.
        </p>
      </div>
    </div>
  )
}
