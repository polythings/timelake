"use client"

import {
  Archive,
  Bot,
  BotMessageSquare,
  ChartCandlestick,
  HardDrive,
  Lambda,
  LayoutDashboard,
  Radio,
  Server,
  Siren,
  type LucideIcon,
} from "lucide-react"
import { useLayoutEffect, useRef, useState } from "react"

import { TimelakeMark } from "@/components/brand/timelake-mark"
import { DotGrid, type Segment } from "@/components/dots"
import { cn } from "@/lib/utils"

/**
 * The stack as a node graph: publishers → ingest → Timelake → readers. Nodes are laid out with CSS; the connector
 * lines are measured from the nodes and drawn in SVG, left to right on wide screens and down a bus on narrow ones.
 * Dashed nodes and lines are planned.
 */
type Endpoint = { id: string; icon: LucideIcon; kind: string; name: string; via: string; planned?: boolean }

const PUBLISHERS: Endpoint[] = [
  { id: "device", icon: Radio, kind: "Sensor", name: "Field device", via: "MQTT" },
  { id: "market", icon: ChartCandlestick, kind: "Feed", name: "Market data", via: "REST" },
  { id: "robot", icon: Bot, kind: "Robot", name: "Robot arm", via: "ROS 2" },
]
const BACKFILL: Endpoint = { id: "files", icon: Archive, kind: "Files", name: "Backfill", via: "Batch" }

// Alarms and detections from the ingest rules, published to the user's event stream.
const EVENTS: Endpoint = { id: "events", icon: Siren, kind: "Events", name: "Kinesis", via: "Alarms", planned: true }

// Readers that run the library talk to Timelake; the others read the bucket's files directly.
const READERS: (Endpoint & { from: "timelake" | "bucket" })[] = [
  { id: "lambda", icon: Lambda, kind: "Function", name: "AWS Lambda", via: "SQL", from: "timelake" },
  { id: "agent", icon: BotMessageSquare, kind: "AI", name: "AI agent", via: "MCP", planned: true, from: "timelake" },
  { id: "dashboard", icon: LayoutDashboard, kind: "App", name: "Dashboard", via: "HTTPS", from: "bucket" },
  { id: "engines", icon: Server, kind: "Engines", name: "DuckDB · Spark\nPyIceberg", via: "Iceberg", from: "bucket" },
]

/** The library, in the processes that use it. The last row is what Timelake adds. */
const LIBRARY: [string, string][] = [
  ["Use from", "Python · Rust · CLI · TS"],
  ["Read", "SQL on DataFusion"],
]
const WRITE: [string, string] = ["Write", "Sort · compact · index"]
/** What is in the bucket. */
const BUCKET: [string, string][] = [
  ["Storage", "S3 · GCS · Azure · R2"],
  ["Tables", "Apache Iceberg"],
  ["Files", "Parquet"],
  ["Catalog", "Files, no server"],
]

// The stream inside the ingest node, as squares: a lit square for each mark, a dim one for each gap.
const STREAM = [".:~=  .,-:;= .~:", ":=.,~:  .-~:;=  ", " .-~:= .,~:;= .,", ".~;= ,-~:= .:~=."]
const STREAM_SEGMENTS: Segment[] = STREAM.join("")
  .split("")
  .map((ch) =>
    ch === " "
      ? { count: 1, color: "var(--dot)" }
      : { count: 1, color: "var(--primary)", alpha: ".,".includes(ch) ? 0.35 : 0.7 },
  )

type Edge = {
  from: string
  to: string
  label: string
  kind: "in" | "bypass" | "arrow" | "event" | "store" | "out"
  planned?: boolean
}

const EDGES: Edge[] = [
  ...PUBLISHERS.map((p) => ({ from: p.id, to: "ingest", label: p.via, kind: "in" as const, planned: true })),
  { from: BACKFILL.id, to: "timelake", label: BACKFILL.via, kind: "bypass" },
  { from: "ingest", to: "timelake", label: "Arrow", kind: "arrow", planned: true },
  { from: "ingest", to: EVENTS.id, label: EVENTS.via, kind: "event", planned: true },
  { from: "timelake", to: "bucket", label: "PUT · GET", kind: "store" },
  ...READERS.map((r) => ({ from: r.from, to: r.id, label: r.via, kind: "out" as const, planned: r.planned })),
]

/** `v` kept at least `pad` inside `lo`..`hi`. */
const clamp = (v: number, lo: number, hi: number, pad = 16) => Math.min(Math.max(v, lo + pad), hi - pad)

type Drawn = { d: string; lx: number; ly: number; label: string; planned?: boolean }

const WIDE = "(min-width: 1280px)"
const BUS_X = 28

function route(e: Edge, a: DOMRect, b: DOMRect, wide: boolean): Drawn {
  const base = { label: e.label, planned: e.planned }
  const acy = a.top + a.height / 2
  const bcy = b.top + b.height / 2
  if (e.kind === "event") {
    // Ingest to its event stream: up on wide screens (the stream sits above ingest), down on narrow ones.
    const x = wide ? a.left + a.width / 2 : a.left + 24
    const [y1, y2] = wide ? [a.top, b.bottom] : [a.bottom, b.top]
    return { ...base, d: `M${x},${y1} V${y2}`, lx: x, ly: (y1 + y2) / 2 }
  }
  if (e.kind === "store") {
    // Timelake to the bucket below it.
    const x = a.left + a.width / 2
    return { ...base, d: `M${x},${a.bottom} V${b.top}`, lx: x, ly: (a.bottom + b.top) / 2 }
  }
  if (wide) {
    if (e.kind === "in") {
      // Converge on a short bus just before the ingest node; the label sits on the first run.
      const mid = b.left - 14
      return { ...base, d: `M${a.right},${acy} H${mid} V${bcy} H${b.left}`, lx: (a.right + mid) / 2, ly: acy }
    }
    // Everything else runs at the height of the small node, stepping just before the big one if it's out of reach.
    const mid = b.left - 20
    if (e.kind === "out") {
      const y = clamp(bcy, a.top, a.bottom)
      const lx = a.right + Math.min((mid - a.right) / 2, 48)
      return { ...base, d: `M${a.right},${y} H${mid} V${bcy} H${b.left}`, lx, ly: y }
    }
    const y = clamp(acy, b.top, b.bottom)
    const lx = a.right + Math.min((mid - a.right) / 2, 48)
    return { ...base, d: `M${a.right},${acy} H${mid} V${y} H${b.left}`, lx, ly: acy }
  }
  // Narrow: a bus down the left edge; indented nodes hang off it on short stubs that carry the label.
  if (e.kind === "out") {
    return { ...base, d: `M${BUS_X},${a.bottom} V${bcy} H${b.left}`, lx: (BUS_X + b.left) / 2, ly: bcy }
  }
  if (e.kind === "arrow") {
    return { ...base, d: `M${a.left},${acy} H${BUS_X} V${b.top}`, lx: BUS_X, ly: (a.bottom + b.top) / 2 }
  }
  const end = e.kind === "in" ? `V${bcy} H${b.left}` : `V${b.top}`
  return { ...base, d: `M${a.left},${acy} H${BUS_X} ${end}`, lx: (BUS_X + a.left) / 2, ly: acy }
}

function NodeCard({ node }: { node: Endpoint }) {
  const Icon = node.icon
  return (
    <div
      data-node={node.id}
      className={cn(
        "relative flex min-h-14 items-center gap-3 rounded-lg border bg-card px-4 py-3",
        node.planned && "border-dashed",
      )}
    >
      <Icon className="size-4.5 shrink-0 text-muted-foreground" aria-hidden />
      <div className="flex min-w-0 flex-col gap-0.5">
        <div className="text-sm leading-snug whitespace-pre-line text-foreground">{node.name}</div>
        <div className="text-xs text-muted-foreground">
          {node.kind}
          {node.planned ? " · planned" : ""}
        </div>
      </div>
    </div>
  )
}

function Row({ k, v, own }: { k: string; v: string; own?: boolean }) {
  return (
    <div className={cn("grid grid-cols-[5rem_minmax(0,1fr)] items-baseline gap-3 px-5 py-2.5", own && "bg-primary/10")}>
      <div className={cn("text-xs", own ? "text-primary" : "text-muted-foreground")}>{k}</div>
      <div className={cn("text-sm", own ? "text-primary" : "text-foreground")}>{v}</div>
    </div>
  )
}

export function StackDiagram() {
  const box = useRef<HTMLDivElement>(null)
  const [lines, setLines] = useState<{ w: number; h: number; drawn: Drawn[] } | null>(null)

  useLayoutEffect(() => {
    const root = box.current
    if (!root) return
    const measure = () => {
      const o = root.getBoundingClientRect()
      const rel = (id: string) => {
        const r = root.querySelector(`[data-node="${id}"]`)?.getBoundingClientRect()
        return r && new DOMRect(r.left - o.left, r.top - o.top, r.width, r.height)
      }
      const wide = window.matchMedia(WIDE).matches
      const drawn = EDGES.flatMap((e) => {
        // Stacked, every reader hangs off the bus below the bucket.
        const [a, b] = [rel(!wide && e.kind === "out" ? "bucket" : e.from), rel(e.to)]
        return a && b ? [route(e, a, b, wide)] : []
      })
      setLines({ w: o.width, h: o.height, drawn })
    }
    measure()
    const ro = new ResizeObserver(measure)
    ro.observe(root)
    root.querySelectorAll("[data-node]").forEach((el) => ro.observe(el))
    return () => ro.disconnect()
  }, [])

  return (
    <div
      ref={box}
      className="relative mx-auto flex w-full max-w-lg flex-col gap-3 xl:grid xl:max-w-none xl:grid-cols-[12rem_5.5rem_9.5rem_5.5rem_minmax(0,1fr)_5.5rem_13.5rem] xl:items-center xl:gap-0"
    >
      {lines ? (
        <svg
          aria-hidden
          className="pointer-events-none absolute inset-0 text-muted-foreground/50"
          width={lines.w}
          height={lines.h}
        >
          {lines.drawn.map((l) => (
            <path
              key={l.d}
              d={l.d}
              fill="none"
              stroke="currentColor"
              strokeWidth={1.25}
              strokeDasharray={l.planned ? "4 4" : undefined}
            />
          ))}
        </svg>
      ) : null}

      {/* Publishers */}
      <div className="flex flex-col gap-3 pl-24 xl:col-start-1 xl:pl-0">
        {[...PUBLISHERS, BACKFILL].map((p) => (
          <NodeCard key={p.id} node={p} />
        ))}
      </div>

      {/* Ingest, with its event stream (below it when stacked, above it on wide screens) */}
      <div className="relative mt-3 ml-24 flex flex-col gap-12 xl:col-start-3 xl:mt-0 xl:ml-0 xl:-translate-y-4">
        <div
          data-node="ingest"
          className="relative flex flex-col gap-3 overflow-hidden rounded-lg border border-dashed bg-card px-4 py-3"
        >
          <DotGrid segments={STREAM_SEGMENTS} perRow={STREAM[0]?.length ?? 16} dot={4} />
          <div className="flex flex-col gap-0.5">
            <span className="text-sm text-foreground">Ingest</span>
            <span className="text-xs text-muted-foreground">Rules · planned</span>
          </div>
        </div>
        <div className="xl:absolute xl:inset-x-0 xl:bottom-full xl:mb-12">
          <NodeCard node={EVENTS} />
        </div>
      </div>

      {/* Timelake, the library, above the bucket it writes */}
      <div className="mt-10 flex flex-col gap-14 xl:col-start-5 xl:mt-0">
        <div data-node="timelake" className="overflow-hidden rounded-lg border border-primary/50 bg-card">
          <div className="flex items-center gap-2.5 border-b px-5 py-3.5">
            <TimelakeMark className="size-5 text-foreground" />
            <span className="text-lg font-normal">Timelake</span>
            <span className="ml-auto text-xs text-muted-foreground">in your process</span>
          </div>
          <div className="divide-y">
            {LIBRARY.map(([k, v]) => (
              <Row key={k} k={k} v={v} />
            ))}
            <Row k={WRITE[0]} v={WRITE[1]} own />
          </div>
        </div>
        <div data-node="bucket" className="overflow-hidden rounded-lg border bg-card">
          <div className="flex items-center gap-2.5 border-b px-5 py-3.5">
            <HardDrive className="size-5 text-muted-foreground" aria-hidden />
            <span className="text-lg font-normal">Your bucket</span>
            <span className="ml-auto text-xs text-muted-foreground">files only</span>
          </div>
          <div className="divide-y">
            {BUCKET.map(([k, v]) => (
              <Row key={k} k={k} v={v} />
            ))}
          </div>
        </div>
      </div>

      {/* Readers */}
      <div className="mt-6 flex flex-col gap-3 pl-24 xl:col-start-7 xl:mt-0 xl:gap-7 xl:pl-0">
        {READERS.map((r) => (
          <NodeCard key={r.id} node={r} />
        ))}
      </div>

      {lines?.drawn.map((l) => (
        <span
          key={`${l.label}-${l.d}`}
          aria-hidden
          className={cn(
            "pointer-events-none absolute -translate-x-1/2 -translate-y-1/2 rounded-sm border bg-background px-1.5 py-px font-mono text-[10px] tracking-[0.08em] whitespace-nowrap uppercase",
            l.planned ? "border-dashed text-muted-foreground" : "text-foreground",
          )}
          style={{ left: l.lx, top: l.ly }}
        >
          {l.label}
        </span>
      ))}
    </div>
  )
}
