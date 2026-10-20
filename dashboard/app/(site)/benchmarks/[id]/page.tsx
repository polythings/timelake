import { ArrowUpRight, ChevronLeft } from "lucide-react"
import type { Metadata } from "next"
import Link from "next/link"
import { notFound } from "next/navigation"
import { Suspense } from "react"

import { BenchCards } from "@/components/bench-cards"
import { BenchHistory } from "@/components/bench-history"
import { SectionResults } from "@/components/bench-sections"
import { Panel } from "@/components/ranking-card"
import { RunInfoButton } from "@/components/run-info"
import { MoreLink, SectionHead } from "@/components/section-head"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { BENCHMARKS, benchmark } from "@/lib/benchmark-catalog"
import {
  CHECKPOINTS,
  SYSTEMS,
  SYSTEM_COLORS,
  WORKLOADS,
  costRows,
  engineLabel,
  failures,
  history,
  latestByTarget,
  liveRows,
  loadRuns,
  queryHistory,
  runInfo,
  runKey,
  runLabel,
  s3TablesDelay,
  sections,
  targetLabel,
  type Run,
  type Workload,
} from "@/lib/benchmarks"
import { formatBytes, formatNumber, formatTime, timeUnit } from "@/lib/format"
import { REPO_URL } from "@/lib/site"

type Props = { params: Promise<{ id: string }> }

/** Benchmarks with results; the others have only a methodology page. */
const withResults = () => BENCHMARKS.filter((b) => b.workload && WORKLOADS.some((w) => w.id === b.workload))

export function generateStaticParams() {
  return withResults().map((b) => ({ id: b.id }))
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const b = benchmark((await params).id)
  return { title: b ? `${b.name} results` : "Benchmarks" }
}

export default function BenchmarkPage({ params }: Props) {
  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-16 px-4 pt-8 pb-24">
      <Suspense fallback={<div className="h-96 animate-pulse rounded-lg bg-muted" />}>
        <BenchmarkBody params={params} />
      </Suspense>
    </div>
  )
}

/** The benchmark's header and results, once its id is known. */
async function BenchmarkBody({ params }: Props) {
  const b = benchmark((await params).id)
  const workload = WORKLOADS.find((w) => w.id === b?.workload)
  if (!b || !workload) notFound()
  return (
    <>
      <header className="flex flex-col gap-4">
        <Link
          href="/benchmarks"
          className="flex w-fit items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          <ChevronLeft className="size-4" /> Benchmarks
        </Link>
        <h1 className="text-4xl leading-[1.05] font-light tracking-tight sm:text-5xl">{b.name}</h1>
        <p className="max-w-4xl text-sm text-muted-foreground">{b.summary}</p>
        {b.provenance && <p className="max-w-4xl text-xs leading-relaxed text-muted-foreground">{b.provenance}</p>}
        <nav className="flex flex-wrap gap-x-5 gap-y-1">
          <MoreLink href={`/benchmarks/${b.id}/methodology`}>Methodology</MoreLink>
          {b.origin && (
            <a href={b.origin.url} className="flex w-fit items-center gap-1 text-sm hover:underline">
              {b.origin.label} <ArrowUpRight className="size-3.5" />
            </a>
          )}
        </nav>
      </header>

      <Results workload={workload} />
    </>
  )
}

async function Results({ workload }: { workload: Workload }) {
  const runs = await loadRuns()
  const latest = latestByTarget(runs, workload.id)
  const [first] = latest
  if (!first) return <p className="text-sm text-muted-foreground">No runs yet.</p>
  return (
    <>
      <Tabs defaultValue={runKey(first)} className="gap-12">
        <TabsList variant="line" className="flex-wrap">
          {latest.map((r) => (
            <TabsTrigger key={runKey(r)} value={runKey(r)} className="font-mono text-xs tracking-[0.06em] uppercase">
              {runLabel(r)}
            </TabsTrigger>
          ))}
        </TabsList>
        {latest.map((r) => (
          <TabsContent key={runKey(r)} value={runKey(r)} className="flex flex-col gap-16">
            <RunResults run={r} runs={runs} workload={workload} />
          </TabsContent>
        ))}
      </Tabs>
      <OverTime runs={runs} latest={latest} workload={workload} />
    </>
  )
}

/** The newest run on one kind of storage: overview, a section per suite and checkpoint, then ingest and the rest. */
function RunResults({ run, runs, workload }: { run: Run; runs: Run[]; workload: Workload }) {
  const delay = s3TablesDelay(run)
  const failed = failures(run)
  const checked = run.queries.some((q) => q.correct !== undefined)
  // History charts draw each system's own reader, in its color.
  const lines = Object.entries(run.header.readers).map(([system, readers]) => {
    const engine = `${system}+${readers[0]}`
    return { engine, label: engineLabel(engine), color: SYSTEM_COLORS[system] ?? "var(--chart-2)" }
  })
  return (
    <>
      <section className="flex flex-col gap-4">
        <p className="flex flex-wrap items-center gap-x-1.5 text-xs text-muted-foreground">
          Run of {run.header.started.slice(0, 10)}
          <RunInfoButton info={runInfo(run)} label={runLabel(run)} />
          {run.header.variant === "bulk" ? "Higher is better." : "Lower is better."}
          {checked && " Answers are checked against a copy of the data with each row once."}
        </p>
        <BenchCards run={run} queries={false} />
      </section>

      {sections(run, workload).map((s) => (
        <section key={`${s.suite}|${s.checkpoint}`} className="flex flex-col gap-4">
          <SectionHead label={workload.suites.length > 1 ? `${s.suiteLabel} · ${s.label}` : s.label}>
            {CHECKPOINT_NOTES[s.checkpoint]}
          </SectionHead>
          <SectionResults section={s} history={queryHistory(runs, run, s.suite, s.checkpoint)} lines={lines} />
        </section>
      ))}

      {delay && (
        <p className="text-sm text-muted-foreground">
          S3 Tables compacted {delay[0].toFixed(1)}–{delay[1].toFixed(1)} hours after ingest; until then its readers see
          the &quot;right after ingest&quot; numbers.
        </p>
      )}
      <IngestSection run={run} />
      <LiveSection run={run} />
      <ConcurrencySection run={run} />
      <CostSection run={run} />
      {failed.length > 0 && <FailedSection failed={failed} />}
    </>
  )
}

/** Queries that failed or timed out, by reader: they're left out of the rankings. */
function FailedSection({ failed }: { failed: ReturnType<typeof failures> }) {
  const byEngine = Map.groupBy(failed, (f) => f.engine)
  return (
    <section className="flex flex-col gap-4">
      <SectionHead label="Failed or timed out">
        Left out of the rankings above; each query&apos;s list says which readers it lost.
      </SectionHead>
      <Panel title="Per reader">
        <ul className="flex flex-col divide-y">
          {[...byEngine].map(([engine, fs]) => (
            <li key={engine} className="grid gap-x-4 gap-y-1 py-2 sm:grid-cols-[14rem_3rem_minmax(0,1fr)]">
              <span className="text-sm">{engine}</span>
              <span className="font-mono text-xs text-muted-foreground tabular-nums sm:text-right">{fs.length}</span>
              <span className="text-xs text-muted-foreground">
                {fs.map((f) => `${f.query} (${(CHECKPOINTS[f.checkpoint] ?? f.checkpoint).toLowerCase()})`).join(", ")}
              </span>
            </li>
          ))}
        </ul>
      </Panel>
    </section>
  )
}

/** What each checkpoint measures, under its section's title. */
const CHECKPOINT_NOTES: Record<string, string> = {
  maintained:
    "After each system's own maintenance (compaction, or its database doing its work), first run of each query in a fresh process.",
  ingested: "Right after the last commit, before any maintenance: the state readers see while data streams in.",
  "1d": "After a day of data has arrived.",
  "7d": "After seven days of data have arrived.",
}

/** Each system's own reader over time on each kind of storage, and every run. */
function OverTime({ runs, latest, workload }: { runs: Run[]; latest: Run[]; workload: Workload }) {
  const all = runs.filter((r) => r.header.workload === workload.id).toReversed()
  return (
    <section className="flex flex-col gap-4">
      <SectionHead label="Over time">
        Each system&apos;s own reader after maintenance: the geometric mean of first runs over the queries it completed
        in each run. The weekly run adds a point each week.
      </SectionHead>
      <div className="grid gap-4 lg:grid-cols-2">
        {latest
          .filter((r) => !r.header.variant)
          .map((r) => {
            const h = history(runs, workload.id, r.header.target, "maintained")
            return (
              <Panel
                key={r.header.target}
                title={targetLabel(r.header.target)}
                note={`${h.points.length} ${h.points.length === 1 ? "run" : "runs"}`}
              >
                <BenchHistory
                  lines={h.engines.map((e) => ({
                    engine: e,
                    label: engineLabel(e),
                    color: SYSTEM_COLORS[e.split("+", 1)[0] ?? ""] ?? "var(--chart-2)",
                  }))}
                  rows={h.points}
                />
              </Panel>
            )
          })}
      </div>
      <Panel title="All runs" note="Hover or tap ⓘ for the machine, the commit and every version.">
        <ul className="flex flex-col divide-y">
          {all.map((r) => (
            <li key={r.header.id} className="flex flex-wrap items-center gap-x-4 gap-y-1 py-2 text-sm">
              <span className="font-mono text-xs tabular-nums">{r.header.started.slice(0, 10)}</span>
              <span className="min-w-32">{runLabel(r)}</span>
              <span className="min-w-0 flex-1 text-xs text-muted-foreground">
                {Object.keys(r.header.readers)
                  .map((s) => SYSTEMS[s] ?? s)
                  .join(", ")}
              </span>
              <span className="flex items-center gap-2">
                <a
                  className="font-mono text-xs text-muted-foreground hover:text-foreground hover:underline"
                  href={`${REPO_URL}/blob/main/bench/results/runs/${r.header.id}.jsonl`}
                >
                  {r.header.label === "weekly" ? "weekly" : "jsonl"}
                </a>
                <RunInfoButton info={runInfo(r)} label={`${r.header.started.slice(0, 10)} ${runLabel(r)}`} />
              </span>
            </li>
          ))}
        </ul>
      </Panel>
    </section>
  )
}

const seconds = (s: number | undefined) =>
  s == null ? "—" : s < 100 ? `${s.toFixed(1)} s` : s < 600 ? `${Math.round(s)} s` : `${Math.round(s / 60)} min`
const ms = (v: number | undefined) => (v == null ? "—" : `${Math.round(v)} ms`)

const systemName = (system: string) => SYSTEMS[system] ?? system

/** Each system's ingest and maintenance: time committing, commit latency, files and storage after maintenance. */
function IngestSection({ run }: { run: Run }) {
  const ingest = run.phases.filter((p) => p.kind === "ingest")
  if (ingest.length === 0) return null
  const maintain = new Map(run.phases.filter((p) => p.kind === "maintain").map((p) => [p.system, p]))
  // A staged stream (history in bulk, then a live day): commit times are the live day's.
  const staged = ingest.some((p) => p.stages)
  return (
    <section className="flex flex-col gap-4">
      <SectionHead label="Ingest and maintenance">
        Committing is the time spent in all commits together
        {staged ? "; commit times are the live day's, a 5-minute window per commit" : ""}. Storage is all files after
        maintenance, counting replaced files still waiting for cleanup.
      </SectionHead>
      <Panel title="Per system" className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>System</TableHead>
              <TableHead className="text-right">Committing</TableHead>
              <TableHead className="text-right">Commit p50</TableHead>
              {!staged && <TableHead className="text-right">Commit p95</TableHead>}
              <TableHead className="text-right">Maintenance</TableHead>
              <TableHead className="text-right">Files</TableHead>
              <TableHead className="text-right">Storage</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {ingest.map((p) => {
              const m = maintain.get(p.system)
              const after = m ?? p
              return (
                <TableRow key={p.system}>
                  <TableCell className={p.system === "timelake" ? "font-medium" : undefined}>
                    {systemName(p.system)}
                  </TableCell>
                  <TableCell className="text-right font-mono tabular-nums">
                    {seconds(p.commit_seconds ?? p.seconds)}
                  </TableCell>
                  <TableCell className="text-right font-mono tabular-nums">
                    {ms(staged ? p.stages?.live?.commit_ms_p50 : p.commit_ms_p50)}
                  </TableCell>
                  {!staged && (
                    <TableCell className="text-right font-mono tabular-nums">{ms(p.commit_ms_p95)}</TableCell>
                  )}
                  <TableCell className="text-right font-mono tabular-nums">
                    {p.system === "s3tables" ? "managed" : seconds(m?.seconds)}
                  </TableCell>
                  <TableCell className="text-right font-mono tabular-nums">
                    {after.data_files == null ? "—" : formatNumber(after.data_files)}
                  </TableCell>
                  <TableCell className="text-right font-mono tabular-nums">
                    {after.bytes == null ? "—" : formatBytes(after.bytes)}
                  </TableCell>
                </TableRow>
              )
            })}
          </TableBody>
        </Table>
      </Panel>
    </section>
  )
}

/** Fresh readers during ingest: how often they were behind, and dashboard queries under writes. */
function LiveSection({ run }: { run: Run }) {
  const rows = liveRows(run)
  if (rows.length === 0) return null
  const unit = timeUnit(rows.flatMap((r) => (r.queryMedian == null ? [] : [r.queryMedian])))
  return (
    <section className="flex flex-col gap-4">
      <SectionHead label="While ingesting">
        Every {run.header.live_interval_s} s during ingest, a fresh reader ran the dashboard queries and checked the
        newest row it could see. Behind: probes that missed a commit that had already finished.
      </SectionHead>
      <Panel title="Per system" className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>System</TableHead>
              <TableHead className="text-right">Probes behind</TableHead>
              <TableHead className="text-right">Most behind</TableHead>
              <TableHead className="text-right">Dashboard query, median</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((r) => (
              <TableRow key={r.system}>
                <TableCell className={r.system === "timelake" ? "font-medium" : undefined}>
                  {systemName(r.system)}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {r.behind} of {r.probes}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {r.behind ? `${r.maxLag.toFixed(1)} s` : "—"}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {r.queryMedian == null ? "—" : formatTime(r.queryMedian, unit)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </Panel>
    </section>
  )
}

/** N dashboard clients at once: throughput and slow queries per client count. */
function ConcurrencySection({ run }: { run: Run }) {
  if (run.concurrency.length === 0) return null
  const counts = [...new Set(run.concurrency.map((c) => c.clients))].sort((a, b) => a - b)
  const systems = [...new Set(run.concurrency.map((c) => c.system))]
  const unit = timeUnit(run.concurrency.flatMap((c) => (c.p95_s == null ? [] : [c.p95_s])))
  return (
    <section className="flex flex-col gap-4">
      <SectionHead label="Concurrent dashboards">
        After maintenance, {counts.join(", ")} clients at once, each with its own reader open, running the dashboard
        queries in turn for {run.concurrency[0]?.seconds} s. Queries per second over all clients, and the 95th
        percentile latency.
      </SectionHead>
      <Panel title="Per system" className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>System</TableHead>
              {counts.map((n) => (
                <TableHead key={n} className="text-right">
                  {n} {n === 1 ? "client" : "clients"}
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {systems.map((system) => (
              <TableRow key={system}>
                <TableCell className={system === "timelake" ? "font-medium" : undefined}>
                  {systemName(system)}
                </TableCell>
                {counts.map((n) => {
                  const c = run.concurrency.find((x) => x.system === system && x.clients === n)
                  return (
                    <TableCell key={n} className="text-right font-mono whitespace-nowrap tabular-nums">
                      {c ? `${c.qps.toFixed(1)} q/s · ${c.p95_s == null ? "—" : formatTime(c.p95_s, unit)}` : "—"}
                      {c && c.errors > 0 ? ` (${c.errors} errors)` : ""}
                    </TableCell>
                  )
                })}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </Panel>
    </section>
  )
}

const usd = (x: number | null) => (x == null ? "—" : x < 0.01 ? `$${x.toFixed(4)}` : `$${x.toFixed(2)}`)

/** What the object-store traffic and storage cost at list prices. */
function CostSection({ run }: { run: Run }) {
  const rows = costRows(run)
  if (rows.length === 0) return null
  return (
    <section className="flex flex-col gap-4">
      <SectionHead label={`Cost on ${targetLabel(run.header.target)}`}>
        Requests and storage at the store&apos;s list prices; machines are not included. Ingest is scaled to a month of
        data arriving at this rate.
      </SectionHead>
      <Panel title="Per system" className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>System</TableHead>
              <TableHead className="text-right">Ingest, per month</TableHead>
              <TableHead className="text-right">Storage, per month</TableHead>
              <TableHead className="text-right">1M queries</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((r) => (
              <TableRow key={r.system}>
                <TableCell className={r.system === "timelake" ? "font-medium" : undefined}>
                  {systemName(r.system)}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">{usd(r.ingestMonth)}</TableCell>
                <TableCell className="text-right font-mono tabular-nums">{usd(r.storageMonth)}</TableCell>
                <TableCell className="text-right font-mono tabular-nums">{usd(r.perMillionQueries)}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </Panel>
    </section>
  )
}
