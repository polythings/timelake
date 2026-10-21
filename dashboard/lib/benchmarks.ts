import "server-only"
import { readdir, readFile } from "node:fs/promises"
import path from "node:path"

import { cacheLife } from "next/cache"
import { z } from "zod"

import type { Racer } from "@/components/race"

import QUERY_FILES from "./bench-queries.json"
import {
  TIMELAKE_ENGINE,
  engineLabel,
  engineOf,
  engineOrder,
  ownEngine,
  systemColor,
  systemLabel,
  systemOf,
  type Rank,
  type Ranking,
  type SystemsMeta,
} from "./bench-systems"

export { engineLabel, type Rank, type Ranking } from "./bench-systems"

/** Run files bundled with the app (bun run sync-results copies them from ../bench/results/runs). */
const RUNS = process.env.TIMELAKE_BENCH_RUNS ?? path.resolve(/*turbopackIgnore: true*/ process.cwd(), "data/runs")

/** The first line of a run file: what ran, where, and with which code. */
const RunHeaderSchema = z.object({
  id: z.string(),
  workload: z.string(),
  scale: z.string(),
  params: z.record(z.string(), z.unknown()),
  target: z.string(),
  storage: z.string(),
  machine: z.string(),
  commit: z.string(),
  dirty: z.boolean(),
  label: z.string(),
  /** A kind of run kept apart from the others of its workload and target, e.g. "bulk". */
  variant: z.string().optional(),
  /** `tuned` or `default` (D23); absent in runs before 2026-10-20, which count as tuned. */
  tuning: z.string().optional(),
  started: z.string(),
  readers: z.record(z.string(), z.array(z.string())),
  /** Each system's title and readers (timebench's), absent in runs before 2026-10-21. */
  systems: z.record(z.string(), z.object({ title: z.string(), readers: z.array(z.string()) })).optional(),
  versions: z.record(z.string(), z.string()),
  runs: z.number(),
  live_interval_s: z.number().optional(),
  concurrency: z.array(z.number()).optional(),
  imported: z.string().optional(),
})
export type RunHeader = z.infer<typeof RunHeaderSchema>

const StageSchema = z.object({
  rows: z.number(),
  commits: z.number(),
  seconds: z.number(),
  commit_ms_p50: z.number().optional(),
})
export type StageRecord = z.infer<typeof StageSchema>

const PhaseSchema = z.object({
  kind: z.enum(["ingest", "maintain"]),
  system: z.string(),
  status: z.string(),
  error: z.string().optional(),
  seconds: z.number().optional(),
  /** Time spent committing; with pacing, `seconds` is mostly waiting between arrivals. */
  commit_seconds: z.number().optional(),
  commits: z.number().optional(),
  rows: z.number().optional(),
  commit_ms_p50: z.number().optional(),
  commit_ms_p95: z.number().optional(),
  objects: z.number().nullable().optional(),
  bytes: z.number().optional(),
  data_files: z.number().optional(),
  data_bytes: z.number().optional(),
  requests: z.number().optional(),
  compaction_after_ingest_s: z.record(z.string(), z.number()).optional(),
  /** The phase's requests at the store's list prices. */
  usd: z.number().optional(),
  /** Keeping the tables a month at the store's list prices. */
  storage_usd_month: z.number().optional(),
  /** Per stage of a staged stream (bulk history, then a live day). */
  stages: z.record(z.string(), StageSchema).optional(),
})
export type PhaseRecord = z.infer<typeof PhaseSchema>

/** Fresh readers probing a system while it ingests. */
const LiveSchema = z.object({
  system: z.string(),
  reader: z.string(),
  interval_s: z.number(),
  rounds: z.number(),
  /** Per freshness probe: seconds behind the oldest commit it missed (0 = up to date). */
  lag_s: z.array(z.number()),
  /** Dashboard query -> first-run seconds of each probe. */
  queries: z.record(z.string(), z.array(z.number())),
  errors: z.array(z.string()),
})
export type LiveRecord = z.infer<typeof LiveSchema>

/** N dashboard clients querying at once. */
const ConcurrencySchema = z.object({
  system: z.string(),
  reader: z.string(),
  clients: z.number(),
  seconds: z.number(),
  queries: z.number(),
  qps: z.number(),
  p50_s: z.number().optional(),
  p95_s: z.number().optional(),
  errors: z.number(),
})
export type ConcurrencyRecord = z.infer<typeof ConcurrencySchema>

const QuerySchema = z.object({
  system: z.string(),
  reader: z.string(),
  checkpoint: z.string(),
  suite: z.string(),
  query: z.string(),
  status: z.string(),
  error: z.string().optional(),
  /** Seconds per run: the first run, then the repeats. Empty (or absent in the file) where it failed before a run finished. */
  times: z.array(z.number()).default([]),
  rows: z.number().nullable().optional(),
  /** Requests per run. */
  requests: z.array(z.number()).optional(),
  /** Bytes read from storage, per run. */
  bytes: z.array(z.number()).optional(),
  server_ms: z.array(z.number()).optional(),
  /** The first run's requests at the store's list prices. */
  usd: z.number().optional(),
  /** Whether the answer matched the reference system's (absent when the run had none). */
  correct: z.boolean().optional(),
})
export type QueryRecord = z.infer<typeof QuerySchema>

/** A query that answered: at least a first run. */
type Times = [first: number, ...repeats: number[]]
export type OkQuery = QueryRecord & { status: "ok"; times: Times }
export const isOk = (q: QueryRecord): q is OkQuery => q.status === "ok" && q.times.length > 0

/** Any line of a run file: the header or a record, told apart by `kind`. */
const LineSchema = z.looseObject({ kind: z.string(), system: z.string().optional() })
const VersionsSchema = z.object({ versions: z.record(z.string(), z.string()) })
const EndSchema = z.object({ finished: z.string() })

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

/** Each suite's queries as the query files describe them (bench/queries; timebench queries --site). */
const DESCRIBED = z
  .record(
    z.string(),
    z.array(z.object({ name: z.string(), description: z.string(), sql: z.string(), tuned: z.string().optional() })),
  )
  .parse(QUERY_FILES)

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

/** What each checkpoint measures, under its section's title. */
export const CHECKPOINT_NOTES: Record<string, string> = {
  maintained:
    "After each system's maintenance, first run of each query in a fresh process. Systems maintain at different times: Timelake while it ingests, most others in a separate step after it, some not at all; the panel below says which, and how long the step after ingest took.",
  ingested: "Right after the last commit, before any maintenance: the state readers see while data streams in.",
  "1d": "After a day of data has arrived.",
  "7d": "After seven days of data have arrived.",
}

/** The checkpoint right after the last commit, before any maintenance. */
const INGESTED = "ingested"

/** The checkpoints a run's top-level results show (the others are on its detail page). */
const MAIN_CHECKPOINTS = ["maintained", INGESTED]

/** The system whose answers the others are checked against; it keeps the right answers and isn't ranked. */
const REFERENCE = "reference"

export const targetLabel = (target: string) => TARGETS[target] ?? target

/** A run's date, e.g. "2026-10-19". */
export const runDate = (run: Run) => run.header.started.slice(0, 10)

/** Whether a run checked answers against the reference. */
export const answersChecked = (run: Run) => run.queries.some((q) => q.correct !== undefined)

/** Said under results whose answers were checked. */
export const CHECKED_NOTE = "Answers are checked against a copy of the data with each row once."

/** The systems a run has, by name: "Timelake, Iceberg (PyIceberg), Lance". */
export const systemList = (run: Run) =>
  Object.keys(run.header.readers)
    .map((s) => systemLabel(s, run.header.systems))
    .join(", ")

async function readRun(file: string): Promise<Run> {
  const lines = (await readFile(path.join(/*turbopackIgnore: true*/ RUNS, file), "utf8"))
    .split("\n")
    .filter((l) => l.trim())
    .map((l) => LineSchema.parse(JSON.parse(l)))
  const [first, ...records] = lines
  const header = RunHeaderSchema.parse(first)
  const of = <T>(kinds: string[], schema: z.ZodType<T>) =>
    records.filter((r) => kinds.includes(r.kind) && r.system !== REFERENCE).map((r) => schema.parse(r))
  const readers = Object.fromEntries(Object.entries(header.readers).filter(([system]) => system !== REFERENCE))
  // Database servers started only for their system record their versions once up.
  const versions = { ...header.versions }
  for (const r of records) if (r.kind === "versions") Object.assign(versions, VersionsSchema.parse(r).versions)
  const end = records.find((r) => r.kind === "end")
  return {
    header: { ...header, readers, versions },
    phases: of(["ingest", "maintain"], PhaseSchema),
    queries: of(["query"], QuerySchema),
    live: of(["live"], LiveSchema),
    concurrency: of(["concurrency"], ConcurrencySchema),
    finished: end ? EndSchema.parse(end).finished : null,
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
  /** What the query asks, its SQL, and Timelake's version in a tuned run (D23), where the query files have them. */
  description?: string
  sql?: string
  tuned?: string
  /** Each engine's first run on this query, fastest first; engines that failed it are left out. */
  ranking: Ranking
  /** Engines (`system+reader`) that failed or timed out on it. */
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

/** An engine's row in a ranking of query times over `rs`: first runs, best repeats and requests, averaged. */
const rank = (engine: string, rs: OkQuery[], systems?: SystemsMeta): Rank => {
  const requests = rs.map((r) => r.requests?.[0])
  const usd = rs.map((r) => r.usd)
  const bytes = rs.map((r) => r.bytes?.[0])
  const mean = (xs: (number | undefined)[]) =>
    xs.length > 0 && xs.every((x) => x !== undefined)
      ? xs.reduce((s: number, x) => s + (x ?? 0), 0) / xs.length
      : undefined
  return {
    key: engine,
    label: engineLabel(engine, systems),
    system: systemOf(engine),
    value: gm(rs.map((r) => r.times[0])),
    repeat: gm(rs.map((r) => best(r.times))),
    requests: mean(requests),
    usd: mean(usd),
    bytes: mean(bytes),
    timelake: engine === TIMELAKE_ENGINE,
    wrong: rs.filter((r) => r.correct === false).length,
  }
}

/**
 * The ranking of `engines` by geometric-mean first run over the queries all of them answered (so the bars compare the
 * same work), fastest first. `keys` are the queries asked, each found in `by` at `engine|key`; `head` names the
 * ranking given the queries in common. Null where there are none.
 */
function queryRanking(
  engines: string[],
  keys: string[],
  by: Map<string, OkQuery>,
  head: (common: string[]) => Pick<Ranking, "id" | "title" | "note">,
  systems?: SystemsMeta,
): Ranking | null {
  const common = keys.filter((k) => engines.every((e) => by.has(`${e}|${k}`)))
  if (common.length === 0) return null
  return {
    ...head(common),
    unit: "seconds",
    queries: common.length,
    rows: engines
      .map((e) =>
        rank(
          e,
          common.flatMap((k) => by.get(`${e}|${k}`) ?? []),
          systems,
        ),
      )
      .sort((a, b) => a.value - b.value),
  }
}

/**
 * A run's results by suite and checkpoint: each engine's geometric-mean first run over the queries every engine
 * completed (so the bars compare the same work), and each query's times.
 */
export function sections(run: Run, workload: Workload): Section[] {
  const out: Section[] = []
  for (const suite of workload.suites) {
    const described = new Map((DESCRIBED[suite.id] ?? []).map((q) => [q.name, q]))
    for (const [checkpoint, label] of Object.entries(CHECKPOINTS)) {
      const all = run.queries.filter((q) => q.suite === suite.id && q.checkpoint === checkpoint)
      const ok = all.filter(isOk)
      if (ok.length === 0) continue
      const by = new Map(ok.map((q) => [`${engineOf(q)}|${q.query}`, q]))
      const engines = [...new Set(ok.map(engineOf))].sort((a, b) => engineOrder(a) - engineOrder(b))
      const allQueries = [...new Set(all.map((q) => q.query))]
      const ranking = queryRanking(
        engines,
        allQueries,
        by,
        (common) => ({
          id: "all",
          title: "All queries",
          note: `first run, geometric mean of ${common.length} queries`,
        }),
        run.header.systems,
      )
      if (!ranking) continue
      const queries = allQueries.map((query): QueryResult => ({
        query,
        description: described.get(query)?.description,
        sql: described.get(query)?.sql,
        tuned: described.get(query)?.tuned,
        ranking: {
          id: query,
          title: query,
          note: "first run",
          unit: "seconds",
          rows: engines
            .flatMap((e) => {
              const r = by.get(`${e}|${query}`)
              return r ? [rank(e, [r], run.header.systems)] : []
            })
            .sort((a, b) => a.value - b.value),
        },
        failed: all.filter((q) => q.query === query && q.status !== "ok").map(engineOf),
      }))
      out.push({ suite: suite.id, suiteLabel: suite.label, checkpoint, label, ranking, queries })
    }
  }
  return out
}

/** A section's first runs over time: the geometric mean over its queries, and each query's, per engine. */
export type SectionHistory = { all: HistoryPoint[]; queries: Record<string, HistoryPoint[]> }

/** A history point's run: its id, date and commit. */
const pointOf = (run: Run) => ({ run: run.header.id, date: runDate(run), commit: run.header.commit })

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
    const at = pointOf(r)
    const here = r.queries.filter((q) => q.suite === suite && q.checkpoint === checkpoint)
    if (here.length === 0) continue
    const points = new Map<string, HistoryPoint>()
    const times = new Map<string, number[]>()
    for (const q of here.filter(isOk)) {
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
  date: runDate(run),
  machine: run.header.machine,
  commit: run.header.commit,
  versions: Object.entries(run.header.versions),
  imported: Boolean(run.header.imported),
})

/** A query that failed or timed out: the engine's name, the query, its checkpoint and status. */
export type Failure = { engine: string; query: string; checkpoint: string; status: string }

/** Queries that failed or timed out. */
export function failures(run: Run): Failure[] {
  return run.queries
    .filter((q) => q.status !== "ok")
    .map((q) => ({
      engine: engineLabel(engineOf(q), run.header.systems),
      query: q.query,
      checkpoint: q.checkpoint,
      status: q.status,
    }))
}

/** How the site names run variants. */
const VARIANTS: Record<string, string> = { bulk: "bulk load" }

/** A run's tuning (D23): every system as installed (`default`) or as its docs recommend (`tuned`). */
export const tuningOf = (run: Run) => run.header.tuning ?? "tuned"

export const TUNING_LABELS: Record<string, string> = { tuned: "Tuned", default: "Default" }

/** What each tuning means, said wherever results show one. */
export const TUNING_NOTES: Record<string, string> = {
  default: "each system as installed",
  tuned: "each system as its docs recommend, precomputation included",
}

/** A run's storage and variant, e.g. "AWS S3" or "Local disk · bulk load". */
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
  // Targets the site doesn't name go after the ones it does.
  const target = (r: Run) => (order.includes(r.header.target) ? order.indexOf(r.header.target) : order.length)
  const position = (r: Run) => (tuningOf(r) === "tuned" ? 0 : 1000) + (r.header.variant ? 100 : 0) + target(r)
  return [...latest.values()].sort((a, b) => position(a) - position(b))
}

/** The newest tuned run of a workload on one target, without a variant: what the home page shows. */
export function newestRun(runs: Run[], workload: string, target: string): Run | undefined {
  return latestByTarget(runs, workload).find(
    (r) => r.header.target === target && !r.header.variant && tuningOf(r) === "tuned",
  )
}

export type HistoryPoint = { run: string; date: string; commit: string } & Record<string, number | string>

/** A system's own reader in a run: the one the site ranks first, else the first the run lists. */
export const ownEngineIn = (run: Run, system: string) =>
  ownEngine(system, run.header.systems) ?? `${system}+${run.header.readers[system]?.[0] ?? ""}`

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
    const point: HistoryPoint = pointOf(r)
    for (const system of Object.keys(r.header.readers)) {
      const engine = ownEngineIn(r, system)
      const rs = r.queries.filter(isOk).filter((q) => engineOf(q) === engine && q.checkpoint === checkpoint)
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

/** A system's row in a ranking of its ingest. */
const phaseRank = (p: PhaseRecord, value: number, systems?: SystemsMeta): Rank => ({
  key: p.system,
  label: systemLabel(p.system, systems),
  system: p.system,
  value,
  timelake: p.system === "timelake",
})

/** The id of the top-level ranking of query times at `checkpoint`. */
export const queriesRankingId = (checkpoint: string) => `queries-${checkpoint}`

/**
 * The top-level results of a run: query time after maintenance and right after ingest (geometric mean of first runs
 * over every query all readers completed), then median commit time. Fastest first.
 */
export function rankings(run: Run): Ranking[] {
  const out: Ranking[] = []
  for (const checkpoint of MAIN_CHECKPOINTS) {
    const label = CHECKPOINTS[checkpoint] ?? checkpoint
    const ok = run.queries.filter((q) => q.checkpoint === checkpoint).filter(isOk)
    const engines = [...new Set(ok.map(engineOf))]
    if (engines.length < 2) continue
    const by = new Map(ok.map((q) => [`${engineOf(q)}|${q.suite}|${q.query}`, q]))
    const all = [...new Set(run.queries.filter((q) => q.checkpoint === checkpoint).map((q) => `${q.suite}|${q.query}`))]
    const ranking = queryRanking(
      engines,
      all,
      by,
      (common) => ({
        id: queriesRankingId(checkpoint),
        title: `Queries, ${label.toLowerCase()}`,
        note: `first run, geometric mean of ${common.length} queries${suiteCounts(run, common)}`,
      }),
      run.header.systems,
    )
    if (ranking) out.push(ranking)
  }
  const ingest = run.phases.filter((p) => p.kind === "ingest" && p.status === "ok" && p.commit_ms_p50 != null)
  if (ingest.some((p) => p.stages)) {
    out.push(...stagedIngest(ingest, run.header.systems))
  } else if (run.header.variant === "bulk" && ingest.length > 1) {
    // A bulk load is judged by its rate: rows per second spent committing.
    out.push({
      id: "load",
      title: "Load",
      note: `rows per second, in commits of ${Number(run.header.params.window_minutes ?? 0) / 60} h of data`,
      unit: "rows/s",
      higherIsBetter: true,
      rows: ingest
        .map((p) => phaseRank(p, (p.rows ?? 0) / (p.commit_seconds ?? p.seconds ?? 1), run.header.systems))
        .sort((a, b) => b.value - a.value),
    })
  } else if (ingest.length > 1) {
    out.push({
      id: "commit",
      title: "Commit",
      note: "median time per commit while streaming",
      unit: "milliseconds",
      rows: ingest.map((p) => phaseRank(p, p.commit_ms_p50 ?? 0, run.header.systems)).sort((a, b) => a.value - b.value),
    })
  }
  return out
}

/**
 * A staged stream's ingest: the bulk history's load rate (rows per second over its stages), and the live day's median
 * commit time.
 */
function stagedIngest(ingest: PhaseRecord[], systems?: SystemsMeta): Ranking[] {
  const load = ingest.map((p) => {
    const stages = Object.entries(p.stages ?? {}).flatMap(([name, s]) => (name === "live" ? [] : [s]))
    const seconds = stages.reduce((t, s) => t + s.seconds, 0)
    return phaseRank(p, seconds > 0 ? stages.reduce((t, s) => t + s.rows, 0) / seconds : 0, systems)
  })
  const live = ingest.flatMap((p) => {
    const ms = p.stages?.live?.commit_ms_p50
    return ms == null ? [] : [phaseRank(p, ms, systems)]
  })
  const staged: Ranking[] = [
    {
      id: "load",
      title: "Load",
      note: "rows per second, history in commits of an hour of data",
      unit: "rows/s",
      higherIsBetter: true,
      rows: load.sort((a, b) => b.value - a.value),
    },
    {
      id: "commit",
      title: "Commit",
      note: "median time per commit of the live day",
      unit: "milliseconds",
      rows: live.sort((a, b) => a.value - b.value),
    },
  ]
  return staged.filter((r) => r.rows.length > 1)
}

/** The query the request race runs: the latest position of every truck. */
export const LAST_LOC = "last-loc"

/** The engines in the request race: Timelake's own reader, then the others through DuckDB. */
const RACE_ENGINES = [TIMELAKE_ENGINE, "lance+duckdb", "iceberg+duckdb", "parquet+duckdb"]

/** The race engines' first answers to "last-loc" right after ingest, in race order. */
export function lastLoc(run: Run): { engine: string; query: OkQuery }[] {
  return RACE_ENGINES.flatMap((engine) => {
    const query = run.queries
      .filter(isOk)
      .find((q) => engineOf(q) === engine && q.query === LAST_LOC && q.checkpoint === INGESTED)
    return query ? [{ engine, query }] : []
  })
}

/** How many trucks "last-loc" returned in a run, as Timelake answered it. */
export const lastLocRows = (run: Run) =>
  run.queries.find((q) => q.query === LAST_LOC && q.system === "timelake")?.rows ?? undefined

/**
 * "last-loc" right after ingest as a race: each engine's requests (a dot each), or the data it read (`bytes`: in MB,
 * a dot per 100 KB), at its first run's pace.
 */
export function lastLocRacers(run: Run, measure: "requests" | "bytes" = "requests"): Racer[] {
  return lastLoc(run).flatMap(({ engine, query }) => {
    const n = query[measure]?.[0]
    if (n === undefined) return []
    const [value, dots] = measure === "requests" ? [n, n] : [n / 1e6, Math.max(1, Math.round(n / 1e5))]
    return [
      {
        label: engineLabel(engine, run.header.systems),
        value,
        dots,
        seconds: query.times[0],
        timelake: engine === TIMELAKE_ENGINE,
        color: systemColor(engine),
      },
    ]
  })
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
    const own = ownEngineIn(run, system)
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
