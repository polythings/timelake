import { DotGrid } from "@/components/dots"
import { SYSTEM_ORDER, engineOf, systemColor, systemLabel, systemOf, systemOrder } from "@/lib/bench-systems"
import { type Run, engineLabel, isOk, ownEngineIn } from "@/lib/benchmarks"
import { formatNumber } from "@/lib/format"
import { cn } from "@/lib/utils"

/** Whether `engine` is its system's own reader in `run`. */
const isOwn = (run: Run, engine: string) => ownEngineIn(run, systemOf(engine)) === engine

/** A system's color; a system read by another engine than its own is drawn at half strength. */
export function engineStyle(run: Run, engine: string): { color: string; alpha: number } {
  return { color: systemColor(engine), alpha: isOwn(run, engine) ? 1 : 0.5 }
}

/** The engines a run queried, by system, each system's own reader first. */
function engines(run: Run): string[] {
  const all = [...new Set(run.queries.map(engineOf))]
  const rank = (e: string) => systemOrder(systemOf(e)) * 2 + (isOwn(run, e) ? 0 : 1)
  return all.sort((a, b) => rank(a) - rank(b))
}

/** The queries of a run, in the order it ran them. */
function queries(run: Run): string[] {
  return [...new Set(run.queries.map((q) => q.query))]
}

function Swatch({ color, alpha = 1 }: { color: string; alpha?: number }) {
  return <span className="inline-block size-2.5 shrink-0" style={{ background: color, opacity: alpha }} />
}

/** Every engine's color, as a row of chips. */
export function EngineLegend({ run }: { run: Run }) {
  return (
    <div className="flex flex-wrap gap-x-5 gap-y-2 text-xs text-muted-foreground">
      {engines(run).map((e) => (
        <span key={e} className="flex items-center gap-2">
          <Swatch {...engineStyle(run, e)} />
          {engineLabel(e, run.header.systems)}
        </span>
      ))}
    </div>
  )
}

/** Each system's objects in the bucket after streaming: data files in full color, everything else dimmed. */
export function BucketObjects({ run }: { run: Run }) {
  const rows = SYSTEM_ORDER.flatMap((system) => {
    const p = run.phases.find((x) => x.kind === "ingest" && x.system === system && x.status === "ok")
    if (p?.objects == null || p.data_files == null) return []
    return [{ system, objects: p.objects, data: p.data_files, commits: p.commits ?? 0 }]
  })
  return (
    <div className="flex flex-col gap-5 rounded-md border bg-card p-6">
      {rows.map((r) => {
        const color = systemColor(r.system)
        return (
          <div key={r.system} className="flex flex-col gap-2">
            <div className="flex flex-wrap items-baseline justify-between gap-x-4 text-sm">
              <span className={r.system === "timelake" ? "font-medium" : "text-muted-foreground"}>
                {systemLabel(r.system, run.header.systems)}
              </span>
              <span className="font-mono text-xs text-muted-foreground tabular-nums">
                {formatNumber(r.data)} data + {formatNumber(r.objects - r.data)} other ={" "}
                <span className="text-foreground">{formatNumber(r.objects)} objects</span>
              </span>
            </div>
            <DotGrid
              segments={[
                { count: r.data, color },
                { count: r.objects - r.data, color, alpha: 0.3 },
              ]}
              label={`${systemLabel(r.system, run.header.systems)}: ${r.objects} objects, ${r.data} of them data files`}
            />
          </div>
        )
      })}
      <div className="flex flex-wrap gap-x-5 gap-y-1 border-t pt-4 text-xs text-muted-foreground">
        <span className="flex items-center gap-2">
          <Swatch color="var(--muted-foreground)" /> Data files
        </span>
        <span className="flex items-center gap-2">
          <Swatch color="var(--muted-foreground)" alpha={0.3} /> Metadata, manifests, and indexes
        </span>
        <span>
          After streaming {formatNumber(rows[0]?.commits ?? 0)} commits, before any maintenance job. One square per
          object.
        </span>
      </div>
    </div>
  )
}

/**
 * Every query against every engine at one checkpoint: a square per pair in the engine's color, as bright as it is
 * fast (the fastest on a query is full strength). Hover a square for its time.
 */
export function QueryMatrix({ run, checkpoint, title }: { run: Run; checkpoint: string; title: string }) {
  const es = engines(run)
  const qs = queries(run)
  const time = (e: string, query: string) =>
    run.queries.filter(isOk).find((q) => engineOf(q) === e && q.query === query && q.checkpoint === checkpoint)
      ?.times[0]
  return (
    <div className="flex min-w-0 flex-col gap-3">
      <div className="text-sm font-medium">{title}</div>
      <div
        className="grid items-center gap-x-[3px] gap-y-[3px] overflow-x-auto"
        style={{ gridTemplateColumns: `minmax(10rem, 1fr) repeat(${es.length}, 14px)` }}
      >
        {qs.map((query) => {
          const times = es.map((e) => time(e, query))
          const fastest = Math.min(...times.flatMap((t) => (t === undefined ? [] : [t])))
          return [
            <span key={query} className="truncate pr-2 font-mono text-[11px] text-muted-foreground" title={query}>
              {query}
            </span>,
            ...es.map((e, i) => {
              const t = times[i]
              const { color, alpha } = engineStyle(run, e)
              const ratio = t === undefined ? null : t / fastest
              return (
                <span
                  key={`${query}-${e}`}
                  title={`${engineLabel(e, run.header.systems)} · ${query}: ${t === undefined ? "no result" : `${t.toFixed(3)} s${ratio && ratio > 1.05 ? `, ${ratio.toFixed(1)}× the fastest` : ", fastest"}`}`}
                  className={ratio === null ? "size-3.5 border border-dashed" : "size-3.5"}
                  style={
                    ratio === null ? undefined : { background: color, opacity: Math.max(0.1, alpha / ratio ** 0.6) }
                  }
                />
              )
            }),
          ]
        })}
      </div>
    </div>
  )
}

/** The per-query lanes' columns: the query, then requests and data read; on a phone the query takes a row above. */
const LANES = "grid grid-cols-2 gap-x-4 gap-y-1 sm:grid-cols-[minmax(9rem,12rem)_1fr_1fr] sm:gap-x-6"

/** Units per square in the per-query lanes. */
const PER_REQUESTS = 25
const PER_MB = 3

/**
 * Each query on each system's own reader: requests (a square per 25) and data read (a square per 3 MB), one lane per
 * system.
 */
export function PerQuery({ run, checkpoint }: { run: Run; checkpoint: string }) {
  const own = Object.keys(run.header.readers)
    .sort((a, b) => systemOrder(a) - systemOrder(b))
    .map((s) => ownEngineIn(run, s))
  const find = (e: string, query: string) =>
    run.queries.filter(isOk).find((q) => engineOf(q) === e && q.query === query && q.checkpoint === checkpoint)
  return (
    <div className="flex flex-col gap-4 rounded-md border bg-card p-6">
      <div className={cn(LANES, "font-mono text-[10.5px] tracking-[0.1em] text-muted-foreground uppercase")}>
        <span className="hidden sm:block">Query</span>
        <span>Requests · ■ = {PER_REQUESTS}</span>
        <span>Data read · ■ = {PER_MB} MB</span>
      </div>
      {queries(run).map((query) => (
        <div key={query} className={cn(LANES, "items-start")}>
          <span
            className="col-span-2 truncate pt-px font-mono text-[11px] text-muted-foreground sm:col-span-1"
            title={query}
          >
            {query}
          </span>
          {(["requests", "bytes"] as const).map((kind) => (
            <div key={kind} className="flex min-w-0 flex-col gap-[3px]">
              {own.map((e) => {
                const q = find(e, query)
                const n = q?.[kind]?.[0]
                if (n === undefined) return <span key={e} className="h-[2px]" />
                const value = kind === "requests" ? n : n / 1e6
                const per = kind === "requests" ? PER_REQUESTS : PER_MB
                const { color } = engineStyle(run, e)
                return (
                  <div
                    key={e}
                    title={`${engineLabel(e, run.header.systems)}: ${kind === "requests" ? `${formatNumber(n)} requests` : `${formatNumber(value, 1)} MB`}`}
                  >
                    <DotGrid dot={2} gap={1} segments={[{ count: Math.max(1, Math.round(value / per)), color }]} />
                  </div>
                )
              })}
            </div>
          ))}
        </div>
      ))}
    </div>
  )
}
