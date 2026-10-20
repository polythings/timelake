import "server-only"
import { readdir, readFile } from "node:fs/promises"
import path from "node:path"

import { cacheLife } from "next/cache"

import { ENGINES, SYSTEMS, engineLabel, type Rank, type Ranking } from "./bench-systems"

export { SYSTEM_COLORS, SYSTEMS, engineLabel, type Rank, type Ranking } from "./bench-systems"

/** Run files bundled with the app (bun run sync-results copies them from ../bench/results/runs). */
const RUNS = process.env.TIMELAKE_BENCH_RUNS ?? path.resolve(/*turbopackIgnore: true*/ process.cwd(), "data/runs")

/** Per-run measurements: the first run, then the repeats. */
type Times = [first: number, ...repeats: number[]]

export type RunHeader = {
  id: string
  workload: string
  scale: string
  params: Record<string, unknown>
  target: string
  storage: string
  machine: string
  commit: string
  dirty: boolean
  label: string
  /** A kind of run kept apart from the others of its workload and target, e.g. "bulk". */
  variant?: string
  /** `tuned` or `default` (D23); absent in runs before 2026-10-20, which count as tuned. */
  tuning?: string
  started: string
  readers: Record<string, string[]>
  versions: Record<string, string>
  runs: number
  live_interval_s?: number
  concurrency?: number[]
  imported?: string
}

export type PhaseRecord = {
  kind: "ingest" | "maintain"
  system: string
  status: string
  error?: string
  seconds?: number
  /** Time spent committing; with pacing, `seconds` is mostly waiting between arrivals. */
  commit_seconds?: number
  commits?: number
  rows?: number
  commit_ms_p50?: number
  commit_ms_p95?: number
  objects?: number | null
  bytes?: number
  data_files?: number
  data_bytes?: number
  requests?: number
  compaction_after_ingest_s?: Record<string, number>
  /** The phase's requests at the store's list prices. */
  usd?: number
  /** Keeping the tables a month at the store's list prices. */
  storage_usd_month?: number
  /** Per stage of a staged stream (bulk history, then a live day). */
  stages?: Record<string, StageRecord>
}

export type StageRecord = { rows: number; commits: number; seconds: number; commit_ms_p50?: number }

/** Fresh readers probing a system while it ingests. */
export type LiveRecord = {
  system: string
  reader: string
  interval_s: number
  rounds: number
  /** Per freshness probe: seconds behind the oldest commit it missed (0 = up to date). */
  lag_s: number[]
  /** Dashboard query -> first-run seconds of each probe. */
  queries: Record<string, number[]>
  errors: string[]
}

/** N dashboard clients querying at once. */
export type ConcurrencyRecord = {
  system: string
  reader: string
  clients: number
  seconds: number
  queries: number
  qps: number
  p50_s?: number
  p95_s?: number
  errors: number
}

export type QueryRecord = {
  system: string
  reader: string
  checkpoint: string
  suite: string
  query: string
  status: string
  error?: string
  times: Times
  rows?: number
  requests?: Times
  /** Bytes read from storage, per run. */
  bytes?: Times
  server_ms?: Times
  /** The first run's requests at the store's list prices. */
  usd?: number
  /** Whether the answer matched the reference system's (absent when the run had none). */
  correct?: boolean
}

export type Run = {
  header: RunHeader
  phases: PhaseRecord[]
  queries: QueryRecord[]
  live: LiveRecord[]
  concurrency: ConcurrencyRecord[]
  finished: string | null
}

/** The query suites of each timebench workload, as the detail pages group them. */
export const WORKLOADS = [
  {
    id: "tsbs-iot",
    suites: [
      { id: "tsbs_iot", label: "TSBS IoT (12 queries)" },
      { id: "iot_point", label: "Single truck (5 queries)" },
    ],
  },
  { id: "tsbs-devops", suites: [{ id: "tsbs_devops", label: "TSBS DevOps (15 queries)" }] },
  { id: "iiot", suites: [{ id: "iiot", label: "Industrial IoT (11 queries)" }] },
] as const

export type Workload = (typeof WORKLOADS)[number]

export const TARGETS: Record<string, string> = {
  "aws-s3": "AWS S3",
  "aws-s3x": "S3 Express One Zone",
  local: "Local disk",
  s3: "Local object store",
  "tiger-cloud": "Tiger Cloud",
}

/** Checkpoints in the order pages show them: the main result first. */
export const CHECKPOINTS: Record<string, string> = {
  maintained: "After maintenance",
  ingested: "Right after ingest",
  "1d": "After 1 day",
  "7d": "After 7 days",
}

/** The checkpoints a run's top-level results show (the others are on its detail page). */
const MAIN_CHECKPOINTS = ["maintained", "ingested"]

/** The system whose answers the others are checked against; it keeps the right answers and isn't ranked. */
const REFERENCE = "reference"

export const targetLabel = (target: string) => TARGETS[target] ?? target
const engineOrder = (engine: string) => {
  const i = Object.keys(ENGINES).indexOf(engine)
  return i < 0 ? Object.keys(ENGINES).length : i
}

async function readRun(file: string): Promise<Run> {
  const records = (await readFile(path.join(/*turbopackIgnore: true*/ RUNS, file), "utf8"))
    .split("\n")
    .filter((l) => l.trim())
    .map((l) => JSON.parse(l) as { kind: string } & Record<string, unknown>)
  const end = records.find((r) => r.kind === "end")
  const header = records[0] as unknown as RunHeader
  const readers = Object.fromEntries(Object.entries(header.readers).filter(([system]) => system !== REFERENCE))
  // Database servers started only for their system record their versions once up.
  const versions = Object.assign(
    { ...header.versions },
    ...records.filter((r) => r.kind === "versions").map((r) => r.versions as Record<string, string>),
  ) as Record<string, string>
  const ranked = records.filter((r) => r.system !== REFERENCE)
  return {
    header: { ...header, readers, versions },
    phases: ranked.filter((r) => r.kind === "ingest" || r.kind === "maintain") as unknown as PhaseRecord[],
    queries: ranked.filter((r) => r.kind === "query") as unknown as QueryRecord[],
    live: ranked.filter((r) => r.kind === "live") as unknown as LiveRecord[],
    concurrency: ranked.filter((r) => r.kind === "concurrency") as unknown as ConcurrencyRecord[],
    finished: (end?.finished as string | undefined) ?? null,
  }
}

/** Every run, oldest first. */
export async function loadRuns(): Promise<Run[]> {
  "use cache" // read at build time: runs change only when a benchmark ran (then redeploy)
  cacheLife("max")
  let files: string[]
  try {
    files = (await readdir(/*turbopackIgnore: true*/ RUNS)).filter((f) => f.endsWith(".jsonl")).sort()
  } catch {
    return []
  }
  const runs = await Promise.all(files.map(readRun))
  return runs.sort((a, b) => a.header.started.localeCompare(b.header.started))
}

/** Floor for times in the geometric mean, so a near-zero time doesn't dominate it. */
const MIN_SECONDS = 1e-4

const gm = (xs: number[]) => Math.exp(xs.reduce((s, x) => s + Math.log(Math.max(x, MIN_SECONDS)), 0) / xs.length)

/** The best repeat, or the first run when there were no repeats. */
const best = ([first, ...repeats]: Times) => Math.min(...(repeats.length > 0 ? repeats : [first]))

export type QueryResult = {
  query: string
  /** Each engine's first run on this query, fastest first; engines that failed it are left out. */
  ranking: Ranking
  /** Engines that failed or timed out on it. */
  failed: string[]
}

/** One suite at one checkpoint of a run: its overall ranking and each query's. */
export type Section = {
  suite: string
  suiteLabel: string
  checkpoint: string
  label: string
  /** Geometric mean of first runs over the queries every engine completed. */
  ranking: Ranking
  queries: QueryResult[]
}

const engineOf = (q: QueryRecord) => `${q.system}+${q.reader}`

/** An engine's row in a ranking of query times over `rs`: first runs, best repeats and requests, averaged. */
const rank = (engine: string, rs: QueryRecord[]): Rank => {
  const requests = rs.map((r) => r.requests?.[0])
  const usd = rs.map((r) => r.usd)
  const bytes = rs.map((r) => r.bytes?.[0])
  const mean = (xs: (number | undefined)[]) =>
    xs.length > 0 && xs.every((x) => x !== undefined)
      ? xs.reduce((s: number, x) => s + (x ?? 0), 0) / xs.length
      : undefined
  return {
    key: engine,
    label: engineLabel(engine),
    system: engine.split("+", 1)[0] ?? engine,
    value: gm(rs.map((r) => r.times[0])),
    repeat: gm(rs.map((r) => best(r.times))),
    requests: mean(requests),
    usd: mean(usd),
    bytes: mean(bytes),
    timelake: engine === "timelake+timelake",
    wrong: rs.filter((r) => r.correct === false).length,
  }
}

/**
 * A run's results by suite and checkpoint: each engine's geometric-mean first run over the queries every engine
 * completed (so the bars compare the same work), and each query's times.
 */
export function sections(run: Run, workload: Workload): Section[] {
  const out: Section[] = []
  for (const suite of workload.suites) {
    for (const [checkpoint, label] of Object.entries(CHECKPOINTS)) {
      const all = run.queries.filter((q) => q.suite === suite.id && q.checkpoint === checkpoint)
      const ok = all.filter((q) => q.status === "ok")
      if (ok.length === 0) continue
      const by = new Map(ok.map((q) => [`${engineOf(q)}|${q.query}`, q]))
      const engines = [...new Set(ok.map(engineOf))].sort((a, b) => engineOrder(a) - engineOrder(b))
      const allQueries = [...new Set(all.map((q) => q.query))]
      const common = allQueries.filter((q) => engines.every((e) => by.has(`${e}|${q}`)))
      if (common.length === 0) continue
      const ranking: Ranking = {
        title: "All queries",
        note: `first run, geometric mean of ${common.length} queries`,
        unit: "seconds",
        queries: common.length,
        rows: engines
          .map((e) =>
            rank(
              e,
              common.flatMap((q) => by.get(`${e}|${q}`) ?? []),
            ),
          )
          .sort((a, b) => a.value - b.value),
      }
      const queries = allQueries.map((query) => ({
        query,
        ranking: {
          title: query,
          note: "first run",
          unit: "seconds" as const,
          rows: engines
            .flatMap((e) => {
              const r = by.get(`${e}|${query}`)
              return r ? [rank(e, [r])] : []
            })
            .sort((a, b) => a.value - b.value),
        },
        failed: all.filter((q) => q.query === query && q.status !== "ok").map((q) => engineLabel(engineOf(q))),
      }))
      out.push({ suite: suite.id, suiteLabel: suite.label, checkpoint, label, ranking, queries })
    }
  }
  return out
}

/** A section's first runs over time: the geometric mean over its queries, and each query's, per engine. */
export type SectionHistory = { all: HistoryPoint[]; queries: Record<string, HistoryPoint[]> }

/**
 * A section's history at one suite and checkpoint, across the runs of `run`'s workload, target, variant and tuning:
 * one point per run. An engine's mean is drawn for a run only where it answered every query that run had.
 */
export function sectionHistory(runs: Run[], run: Run, suite: string, checkpoint: string): SectionHistory {
  const same = runs.filter(
    (r) =>
      r.header.workload === run.header.workload &&
      r.header.target === run.header.target &&
      r.header.variant === run.header.variant &&
      tuningOf(r) === tuningOf(run),
  )
  const out: SectionHistory = { all: [], queries: {} }
  for (const r of same) {
    const at = { run: r.header.id, date: r.header.started.slice(0, 10), commit: r.header.commit }
    const here = r.queries.filter((q) => q.suite === suite && q.checkpoint === checkpoint)
    if (here.length === 0) continue
    const points = new Map<string, HistoryPoint>()
    const times = new Map<string, number[]>()
    for (const q of here) {
      if (q.status !== "ok") continue
      const point: HistoryPoint = points.get(q.query) ?? { ...at }
      point[engineOf(q)] = q.times[0]
      points.set(q.query, point)
      times.set(engineOf(q), [...(times.get(engineOf(q)) ?? []), q.times[0]])
    }
    for (const [query, point] of points) (out.queries[query] ??= []).push(point)
    const asked = new Set(here.map((q) => q.query)).size
    const all: HistoryPoint = { ...at }
    for (const [engine, ts] of times) if (ts.length === asked) all[engine] = gm(ts)
    out.all.push(all)
  }
  return out
}

/** A run's provenance, for its info tooltip: when, where, which code and which versions of everything. */
export type RunInfo = { date: string; machine: string; commit: string; versions: [string, string][]; imported: boolean }

export const runInfo = (run: Run): RunInfo => ({
  date: run.header.started.slice(0, 10),
  machine: run.header.machine,
  commit: run.header.commit,
  versions: Object.entries(run.header.versions),
  imported: Boolean(run.header.imported),
})

/** Queries that failed or timed out, as "engine: query (status)". */
export function failures(run: Run): { engine: string; query: string; checkpoint: string; status: string }[] {
  return run.queries
    .filter((q) => q.status !== "ok")
    .map((q) => ({ engine: engineLabel(engineOf(q)), query: q.query, checkpoint: q.checkpoint, status: q.status }))
}

/** How the site names run variants. */
const VARIANTS: Record<string, string> = { bulk: "bulk load" }

/** A run's storage and variant, e.g. "AWS S3" or "Local disk · bulk load". */
/** A run's tuning (D23): every system as installed (`default`) or as its docs recommend (`tuned`). */
export const tuningOf = (run: Run) => run.header.tuning ?? "tuned"

export const TUNING_LABELS: Record<string, string> = { tuned: "Tuned", default: "Default" }

export const runLabel = (run: Run) =>
  targetLabel(run.header.target) +
  (run.header.variant ? ` · ${VARIANTS[run.header.variant] ?? run.header.variant}` : "")

/** Tells runs of a workload apart: target and variant. */
export const runKey = (run: Run) => `${run.header.target}|${run.header.variant ?? ""}|${tuningOf(run)}`

/** The newest run of a workload on each target, variant and tuning: tuned first, the default variant first, then by target. */
export function latestByTarget(runs: Run[], workload: string): Run[] {
  const latest = new Map<string, Run>()
  for (const r of runs) if (r.header.workload === workload) latest.set(runKey(r), r)
  const order = Object.keys(TARGETS)
  const position = (r: Run) =>
    (tuningOf(r) === "tuned" ? 0 : 1000) + (r.header.variant ? 100 : 0) + order.indexOf(r.header.target)
  return [...latest.values()].sort((a, b) => position(a) - position(b))
}

export type HistoryPoint = { run: string; date: string; commit: string } & Record<string, number | string>

/**
 * Each system's own reader over time on one target: geometric-mean first run over all of a run's queries that it
 * completed at a checkpoint. A line is one engine; a point is one run.
 */
export function history(
  runs: Run[],
  workload: string,
  target: string,
  checkpoint: string,
  tuning = "tuned",
): { points: HistoryPoint[]; engines: string[] } {
  const points: HistoryPoint[] = []
  const engines = new Set<string>()
  for (const r of runs) {
    if (r.header.workload !== workload || r.header.target !== target || r.header.variant || tuningOf(r) !== tuning)
      continue
    const point: HistoryPoint = { run: r.header.id, date: r.header.started.slice(0, 10), commit: r.header.commit }
    for (const [system, readers] of Object.entries(r.header.readers)) {
      const engine = `${system}+${readers[0]}`
      const rs = r.queries.filter((q) => engineOf(q) === engine && q.checkpoint === checkpoint && q.status === "ok")
      if (rs.length === 0) continue
      point[engine] = gm(rs.map((q) => q.times[0]))
      engines.add(engine)
    }
    points.push(point)
  }
  return { points, engines: [...engines].sort((a, b) => engineOrder(a) - engineOrder(b)) }
}

/** Hours from the end of ingest until S3 Tables' managed compaction had run, per table: [min, max]. */
export function s3TablesDelay(run: Run): [number, number] | null {
  const m = run.phases.find((p) => p.system === "s3tables" && p.kind === "maintain")
  const s = Object.values(m?.compaction_after_ingest_s ?? {})
  return s.length ? [Math.min(...s) / 3600, Math.max(...s) / 3600] : null
}

/** Where queries come from more than one suite, how many from each: " (12 TSBS IoT, 5 single truck)". */
function suiteCounts(run: Run, queries: string[]): string {
  const suites = WORKLOADS.find((w) => w.id === run.header.workload)?.suites ?? []
  if (suites.length < 2) return ""
  const counts = suites.flatMap((s) => {
    const n = queries.filter((q) => q.startsWith(`${s.id}|`)).length
    return n > 0 ? [`${n} ${s.label.replace(/ \(.*\)$/, "")}`] : []
  })
  return counts.length > 1 ? ` (${counts.join(", ")})` : ""
}

/**
 * The top-level results of a run: query time after maintenance and right after ingest (geometric mean of first runs
 * over every query all readers completed), then median commit time. Fastest first.
 */
export function rankings(run: Run): Ranking[] {
  const out: Ranking[] = []
  for (const checkpoint of MAIN_CHECKPOINTS) {
    const label = CHECKPOINTS[checkpoint] ?? checkpoint
    const ok = run.queries.filter((q) => q.checkpoint === checkpoint && q.status === "ok")
    const engines = [...new Set(ok.map(engineOf))]
    if (engines.length < 2) continue
    const by = new Map(ok.map((q) => [`${engineOf(q)}|${q.suite}|${q.query}`, q]))
    const all = [...new Set(run.queries.filter((q) => q.checkpoint === checkpoint).map((q) => `${q.suite}|${q.query}`))]
    const common = all.filter((q) => engines.every((e) => by.has(`${e}|${q}`)))
    if (common.length === 0) continue
    out.push({
      title: `Queries, ${label.toLowerCase()}`,
      note: `first run, geometric mean of ${common.length} queries${suiteCounts(run, common)}`,
      unit: "seconds",
      queries: common.length,
      rows: engines
        .map((e) =>
          rank(
            e,
            common.flatMap((q) => by.get(`${e}|${q}`) ?? []),
          ),
        )
        .sort((a, b) => a.value - b.value),
    })
  }
  const ingest = run.phases.filter((p) => p.kind === "ingest" && p.status === "ok" && p.commit_ms_p50 != null)
  if (ingest.some((p) => p.stages)) {
    out.push(...stagedIngest(ingest))
  } else if (run.header.variant === "bulk" && ingest.length > 1) {
    // A bulk load is judged by its rate: rows per second spent committing.
    out.push({
      title: "Load",
      note: `rows per second, in commits of ${Number(run.header.params.window_minutes ?? 0) / 60} h of data`,
      unit: "rows/s",
      higherIsBetter: true,
      rows: ingest
        .map((p) => ({
          key: p.system,
          label: SYSTEMS[p.system] ?? p.system,
          system: p.system,
          value: (p.rows ?? 0) / (p.commit_seconds ?? p.seconds ?? 1),
          timelake: p.system === "timelake",
        }))
        .sort((a, b) => b.value - a.value),
    })
  } else if (ingest.length > 1) {
    out.push({
      title: "Commit",
      note: "median time per commit while streaming",
      unit: "milliseconds",
      rows: ingest
        .map((p) => ({
          key: p.system,
          label: SYSTEMS[p.system] ?? p.system,
          system: p.system,
          value: p.commit_ms_p50 ?? 0,
          timelake: p.system === "timelake",
        }))
        .sort((a, b) => a.value - b.value),
    })
  }
  return out
}

/**
 * A staged stream's ingest: the bulk history's load rate (rows per second over its stages), and the live day's median
 * commit time.
 */
function stagedIngest(ingest: PhaseRecord[]): Ranking[] {
  const row = (p: PhaseRecord, value: number): Rank => ({
    key: p.system,
    label: SYSTEMS[p.system] ?? p.system,
    system: p.system,
    value,
    timelake: p.system === "timelake",
  })
  const load = ingest.map((p) => {
    const stages = Object.entries(p.stages ?? {}).flatMap(([name, s]) => (name === "live" ? [] : [s]))
    const seconds = stages.reduce((t, s) => t + s.seconds, 0)
    return row(p, seconds > 0 ? stages.reduce((t, s) => t + s.rows, 0) / seconds : 0)
  })
  const live = ingest.flatMap((p) => {
    const ms = p.stages?.live?.commit_ms_p50
    return ms == null ? [] : [row(p, ms)]
  })
  return [
    {
      title: "Load",
      note: "rows per second, history in commits of an hour of data",
      unit: "rows/s" as const,
      higherIsBetter: true,
      rows: load.sort((a, b) => b.value - a.value),
    },
    {
      title: "Commit",
      note: "median time per commit of the live day",
      unit: "milliseconds" as const,
      rows: live.sort((a, b) => a.value - b.value),
    },
  ].filter((r) => r.rows.length > 1)
}

const median = (xs: number[]) => {
  const sorted = xs.toSorted((a, b) => a - b)
  const mid = Math.floor(sorted.length / 2)
  return sorted.length % 2 ? (sorted[mid] ?? 0) : ((sorted[mid - 1] ?? 0) + (sorted[mid] ?? 0)) / 2
}

export type LiveRow = { system: string; probes: number; behind: number; maxLag: number; queryMedian: number | null }

/** While ingesting: how often fresh readers were behind, and the dashboard queries' median first run. */
export function liveRows(run: Run): LiveRow[] {
  return run.live.map((r) => {
    const times = Object.values(r.queries).flat()
    return {
      system: r.system,
      probes: r.lag_s.length,
      behind: r.lag_s.filter((x) => x > 0).length,
      maxLag: Math.max(0, ...r.lag_s),
      queryMedian: times.length ? median(times) : null,
    }
  })
}

export type CostRow = {
  system: string
  ingestMonth: number | null
  storageMonth: number | null
  perQuery: number | null
}

/**
 * At the store's list prices: ingest requests scaled to a month at the run's data rate, storage after maintenance per
 * month, and one first-run query (the mean over the queries) on the system's own reader after maintenance.
 */
export function costRows(run: Run): CostRow[] {
  const span = Number(run.header.params.span_days ?? 3)
  const rows: CostRow[] = []
  for (const system of Object.keys(run.header.readers)) {
    const ingest = run.phases.find((p) => p.kind === "ingest" && p.system === system)
    const maintain = run.phases.find((p) => p.kind === "maintain" && p.system === system)
    const own = `${system}+${run.header.readers[system]?.[0]}`
    const costs = run.queries.flatMap((q) =>
      engineOf(q) === own && q.checkpoint === "maintained" && q.usd != null ? [q.usd] : [],
    )
    const row = {
      system,
      ingestMonth: ingest?.usd != null ? (ingest.usd * 30) / span : null,
      storageMonth: (maintain ?? ingest)?.storage_usd_month ?? null,
      perQuery: costs.length ? costs.reduce((a, b) => a + b, 0) / costs.length : null,
    }
    if (row.ingestMonth != null || row.storageMonth != null || row.perQuery != null) rows.push(row)
  }
  return rows
}
