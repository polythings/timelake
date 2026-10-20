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
import { READERS, SYSTEM_INFO, readerDescription } from "@/lib/bench-systems"
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
  sectionHistory,
  runInfo,
  runKey,
  runLabel,
  s3TablesDelay,
  sections,
  targetLabel,
  TUNING_LABELS,
  tuningOf,
  type Run,
  type Section,
  type Workload,
} from "@/lib/benchmarks"
import { formatBytes, formatNumber, formatTime } from "@/lib/format"
import { REPO_URL } from "@/lib/site"
import { cn } from "@/lib/utils"

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
  if (latest.length === 0) return <p className="text-sm text-muted-foreground">No runs yet.</p>
  const checked = latest.some((r) => r.queries.some((q) => q.correct !== undefined))
  // The query sections of every storage, each with the runs that have it, in suite then checkpoint order.
  const order = (s: Section) =>
    workload.suites.findIndex((x) => x.id === s.suite) * 100 + Object.keys(CHECKPOINTS).indexOf(s.checkpoint)
  const querySections = new Map<string, { section: Section; variants: Variant[] }>()
  for (const r of latest) {
    for (const s of sections(r, workload)) {
      const key = `${s.suite}|${s.checkpoint}`
      const entry = querySections.get(key) ?? { section: s, variants: [] }
      const results = <SectionResults section={s} history={sectionHistory(runs, r, s.suite, s.checkpoint)} />
      entry.variants.push({
        run: r,
        node:
          s.checkpoint === "maintained" ? (
            <>
              <MaintenancePanel run={r} />
              {results}
            </>
          ) : (
            results
          ),
      })
      querySections.set(key, entry)
    }
  }
  const ingest = latest.filter((r) => r.phases.some((p) => p.kind === "ingest"))
  const live = latest.filter((r) => liveRows(r).length > 0)
  const concurrent = latest.filter((r) => r.concurrency.length > 0)
  const costs = latest.filter((r) => costRows(r).length > 0)
  const failing = latest.filter((r) => failures(r).length > 0)
  const queryTitle = (s: Section) =>
    // The suite's name without its query count, which the card gives.
    workload.suites.length > 1 ? `${s.suiteLabel.replace(/ \(.*\)$/, "")} · ${s.label}` : s.label
  const sorted = [...querySections.values()].sort((a, b) => order(a.section) - order(b.section))
  const jumps = [
    ...sorted.map(({ section }) => queryTitle(section)),
    ...(ingest.length ? ["Ingest and maintenance"] : []),
    ...(live.length ? ["While ingesting"] : []),
    ...(concurrent.length ? ["Concurrent dashboards"] : []),
    ...(costs.length ? ["Cost"] : []),
    ...(failing.length ? ["Failed or timed out"] : []),
    "What each entry means",
    "Over time",
  ]
  return (
    <>
      <nav aria-label="On this page" className="-mt-8 flex flex-wrap gap-x-1 gap-y-1 text-sm">
        {jumps.map((j) => (
          <a
            key={j}
            href={`#${slug(j)}`}
            className="inline-flex min-h-8 items-center rounded-md border px-2.5 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
          >
            {j}
          </a>
        ))}
      </nav>
      {checked && (
        <p className="-mt-8 text-sm text-muted-foreground">
          Answers are checked against a copy of the data with each row once. Each section shows the newest run on the
          storage you pick.
        </p>
      )}
      {sorted.map(({ section: s, variants }) => (
        <ResultSection
          key={`${s.suite}|${s.checkpoint}`}
          title={queryTitle(s)}
          note={CHECKPOINT_NOTES[s.checkpoint]}
          variants={variants}
        />
      ))}
      <ResultSection
        title="Ingest and maintenance"
        note="How each system took the data in and kept it in shape: load rate, commit times, maintenance, and what it stored."
        variants={ingest.map((r) => ({ run: r, node: <IngestContent run={r} /> }))}
      />
      <ResultSection
        title="While ingesting"
        note="Fresh readers during ingest: whether they saw every finished commit, and how fast dashboard queries ran under writes."
        variants={live.map((r) => ({ run: r, node: <LivePanel run={r} /> }))}
      />
      <ResultSection
        title="Concurrent dashboards"
        note="Several clients at once after maintenance, each with its own reader, running the dashboard queries in turn."
        variants={concurrent.map((r) => ({ run: r, node: <ConcurrencyPanel run={r} /> }))}
      />
      <ResultSection
        title="Cost"
        note="Requests and storage at the store's list prices; machines are not included."
        variants={costs.map((r) => ({ run: r, node: <CostPanel run={r} /> }))}
      />
      <ResultSection
        title="Failed or timed out"
        note="Left out of the rankings above; each query's list says which readers it lost."
        variants={failing.map((r) => ({ run: r, node: <FailedPanel failed={failures(r)} /> }))}
      />
      <EntriesSection latest={latest} />
      <OverTime runs={runs} latest={latest} workload={workload} />
    </>
  )
}

/**
 * What every entry in the results is: each system, how it's written and maintained, and what reads it. A ranking's
 * entry links here (`#entry-<system>`).
 */
function EntriesSection({ latest }: { latest: Run[] }) {
  const systems = new Map<string, Set<string>>()
  for (const r of latest) {
    for (const [system, readers] of Object.entries(r.header.readers)) {
      const known = systems.get(system) ?? new Set<string>()
      for (const reader of readers) known.add(reader)
      systems.set(system, known)
    }
  }
  const order = Object.keys(SYSTEM_INFO)
  const listed = [...systems].sort(([a], [b]) => order.indexOf(a) - order.indexOf(b))
  return (
    <section id={slug("What each entry means")} className="flex scroll-mt-20 flex-col gap-4">
      <SectionHead label="What each entry means">
        Each system is written, maintained, and read the way its own docs suggest, on the same data. An entry in a
        ranking is a system, or a system and the reader that queried it, such as Lance + DuckDB.
      </SectionHead>
      <p id="tunings" className="scroll-mt-20 text-sm text-muted-foreground">
        Every system runs in two tunings. <span className="text-foreground">Tuned</span>: {TUNING_NOTES.tuned}.{" "}
        <span className="text-foreground">Default</span>: {TUNING_NOTES.default}. Results are tuned unless a
        section&apos;s switch says otherwise.
      </p>
      <div className="grid gap-4 lg:grid-cols-2">
        {listed.map(([system, readers]) => {
          const info = SYSTEM_INFO[system]
          return (
            <div key={system} id={`entry-${system}`} className="scroll-mt-20 rounded-lg target:ring-2 target:ring-ring">
              <Panel title={SYSTEMS[system] ?? system} note={info?.summary} className="h-full">
                <dl className="grid gap-x-4 gap-y-2 text-sm sm:grid-cols-[6.5rem_minmax(0,1fr)]">
                  {info && (
                    <>
                      <dt className="text-muted-foreground">Written</dt>
                      <dd>{info.writes}</dd>
                      <dt className="text-muted-foreground">Maintenance</dt>
                      <dd>{info.maintenance}</dd>
                      <dt className="text-muted-foreground">When</dt>
                      <dd>{info.when}</dd>
                    </>
                  )}
                  <dt className="text-muted-foreground">Read by</dt>
                  <dd className="flex flex-col gap-1">
                    {[...readers].map((reader) => (
                      <span key={reader}>
                        <span className="font-medium">{READERS[reader] ?? reader}</span>:{" "}
                        {readerDescription(system, reader)}
                      </span>
                    ))}
                  </dd>
                </dl>
              </Panel>
            </div>
          )
        })}
      </div>
    </section>
  )
}

/** When each system maintained its tables before the queries after maintenance, and how long its step after ingest took. */
function MaintenancePanel({ run }: { run: Run }) {
  const steps = run.phases.filter((p) => p.kind === "maintain" && p.status === "ok")
  if (steps.length === 0) return null
  return (
    <Panel
      title="Maintenance before these queries"
      note="When each system maintains its tables, and how long its step after the last commit took."
    >
      <ul className="flex flex-col divide-y">
        {steps.map((p) => (
          <li key={p.system} className="grid gap-x-4 gap-y-1 py-2 sm:grid-cols-[14rem_minmax(0,1fr)_5rem]">
            <span className={p.system === "timelake" ? "text-sm" : "text-sm text-muted-foreground"}>
              {systemName(p.system)}
            </span>
            <span className="text-xs text-muted-foreground">{SYSTEM_INFO[p.system]?.when}</span>
            <span className="font-mono text-xs tabular-nums sm:text-right">
              {p.system === "s3tables"
                ? "managed"
                : SYSTEM_INFO[p.system]?.when.startsWith("None")
                  ? "—"
                  : seconds(p.seconds)}
            </span>
          </li>
        ))}
      </ul>
    </Panel>
  )
}

/** One storage's run of a section, and what it shows. */
type Variant = { run: Run; node: React.ReactNode }

/** What each tuning means, said wherever results show one. */
const TUNING_NOTES: Record<string, string> = {
  default: "each system as installed",
  tuned: "each system as its docs recommend, precomputation included",
}

/**
 * A section of results with its own choice of tuning (where both have run) and storage (where more than one has): a
 * tab per kind of storage that has the section's data (AWS S3 first), each showing that run's results, its date, and
 * its details behind ⓘ.
 */
function ResultSection({ title, note, variants }: { title: string; note?: string; variants: Variant[] }) {
  if (variants.length === 0) return null
  const tunings = [...new Set(variants.map((v) => tuningOf(v.run)))]
  return (
    <section id={slug(title)} className="flex scroll-mt-20 flex-col gap-4">
      <SectionHead label={title}>{note}</SectionHead>
      {tunings.length > 1 ? (
        <Tabs defaultValue={tunings[0]} className="gap-4">
          <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
            <TabsList aria-label="Tuning">
              {tunings.map((t) => (
                <TabsTrigger key={t} value={t} className="min-h-8 px-2.5 sm:min-h-7">
                  {TUNING_LABELS[t] ?? t}
                </TabsTrigger>
              ))}
            </TabsList>
            <p className="text-xs text-muted-foreground">
              {tunings.map((t) => `${TUNING_LABELS[t] ?? t}: ${TUNING_NOTES[t] ?? t}.`).join(" ")}
            </p>
          </div>
          {tunings.map((t) => (
            <TabsContent key={t} value={t}>
              <StorageTabs variants={variants.filter((v) => tuningOf(v.run) === t)} />
            </TabsContent>
          ))}
        </Tabs>
      ) : (
        <StorageTabs variants={variants} tuning={tunings[0]} />
      )}
    </section>
  )
}

/**
 * A run's line above its results: its storage where there's no tab to say it, its tuning where there's no switch to
 * say it, its date, and its details behind ⓘ.
 */
function RunLine({ run, storage, tuning }: { run: Run; storage?: boolean; tuning?: string }) {
  return (
    <p className="flex flex-wrap items-center gap-x-1.5 gap-y-1 text-xs text-muted-foreground">
      {storage && <span className="font-mono tracking-[0.06em] text-foreground uppercase">{runLabel(run)}</span>}
      {storage && <span aria-hidden>·</span>}
      {tuning && (
        <>
          <a href="#tunings" className="text-foreground underline decoration-dotted underline-offset-4">
            {TUNING_LABELS[tuning] ?? tuning}
          </a>
          <span aria-hidden>·</span>
        </>
      )}
      Run of {run.header.started.slice(0, 10)}
      <RunInfoButton info={runInfo(run)} label={runLabel(run)} />
    </p>
  )
}

/** A tab per kind of storage, each with its run's line and results; one storage gets its line and no tabs. */
function StorageTabs({ variants, tuning }: { variants: Variant[]; tuning?: string }) {
  const [first] = variants
  if (!first) return null
  if (variants.length === 1)
    return (
      <div className="flex flex-col gap-4">
        <RunLine run={first.run} storage tuning={tuning} />
        {first.node}
      </div>
    )
  return (
    <Tabs defaultValue={runKey(first.run)} className="gap-4">
      <TabsList variant="line" className="flex-wrap" aria-label="Storage">
        {variants.map((v) => (
          <TabsTrigger
            key={runKey(v.run)}
            value={runKey(v.run)}
            className="min-h-8 font-mono text-xs tracking-[0.06em] uppercase"
          >
            {runLabel(v.run)}
          </TabsTrigger>
        ))}
      </TabsList>
      {variants.map((v) => (
        <TabsContent key={runKey(v.run)} value={runKey(v.run)} className="flex flex-col gap-4">
          <RunLine run={v.run} tuning={tuning} />
          {v.node}
        </TabsContent>
      ))}
    </Tabs>
  )
}

/** A run's ingest: load and commit rankings, the requests one query takes, and the per-system table. */
function IngestContent({ run }: { run: Run }) {
  const delay = s3TablesDelay(run)
  return (
    <>
      <BenchCards run={run} queries={false} />
      <IngestPanel run={run} />
      {delay && (
        <p className="text-sm text-muted-foreground">
          S3 Tables compacted {delay[0].toFixed(1)}–{delay[1].toFixed(1)} hours after ingest; until then its readers see
          the &quot;right after ingest&quot; numbers.
        </p>
      )}
    </>
  )
}

/** Queries that failed or timed out, by reader: they're left out of the rankings. */
function FailedPanel({ failed }: { failed: ReturnType<typeof failures> }) {
  const byEngine = Map.groupBy(failed, (f) => f.engine)
  return (
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
  )
}

/** What each checkpoint measures, under its section's title. */
const CHECKPOINT_NOTES: Record<string, string> = {
  maintained:
    "After each system's maintenance, first run of each query in a fresh process. Systems maintain at different times: Timelake while it ingests, most others in a separate step after it, some not at all; the panel below says which, and how long the step after ingest took.",
  ingested: "Right after the last commit, before any maintenance: the state readers see while data streams in.",
  "1d": "After a day of data has arrived.",
  "7d": "After seven days of data have arrived.",
}

/** Each system's own reader over time on each kind of storage, and every run. */
function OverTime({ runs, latest, workload }: { runs: Run[]; latest: Run[]; workload: Workload }) {
  const all = runs.filter((r) => r.header.workload === workload.id).toReversed()
  return (
    <section id={slug("Over time")} className="flex scroll-mt-20 flex-col gap-4">
      <SectionHead label="Over time">
        Each system&apos;s own reader after maintenance: the geometric mean of first runs over the queries it completed
        in each run. The weekly run adds a point each week.
      </SectionHead>
      <div className="grid gap-4 lg:grid-cols-2">
        {latest
          .filter((r) => !r.header.variant && tuningOf(r) === "tuned")
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
              <span className="min-w-32">
                {runLabel(r)}
                {tuningOf(r) === "tuned" ? "" : ` · ${TUNING_LABELS[tuningOf(r)]?.toLowerCase() ?? tuningOf(r)}`}
              </span>
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

/** A section title as a fragment id: "TSBS IoT · After maintenance" -> "tsbs-iot-after-maintenance". */
const slug = (title: string) =>
  title
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")

/** A table's first column stays in place while the rest scrolls sideways on narrow screens. */
const FIRST = "sticky left-0 z-10 max-w-32 min-w-24 bg-card whitespace-normal sm:max-w-none"

/** Under a table that may scroll sideways on a phone: says so. */
function ScrollHint() {
  return <p className="text-[11px] text-muted-foreground sm:hidden">Scroll the table sideways for every column.</p>
}

/** Each system's ingest and maintenance: time committing, commit latency, files and storage after maintenance. */
function IngestPanel({ run }: { run: Run }) {
  const ingest = run.phases.filter((p) => p.kind === "ingest")
  if (ingest.length === 0) return null
  const maintain = new Map(run.phases.filter((p) => p.kind === "maintain").map((p) => [p.system, p]))
  // A staged stream (history in bulk, then a live day): commit times are the live day's.
  const staged = ingest.some((p) => p.stages)
  return (
    <Panel
      title="Per system"
      note={
        <>
          Committing is the time spent in all commits together
          {staged ? "; commit times are the live day's, a 5-minute window per commit" : ""}. Storage is all files after
          maintenance, counting replaced files still waiting for cleanup.
        </>
      }
      className="overflow-x-auto"
    >
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className={FIRST}>System</TableHead>
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
            if (p.status !== "ok")
              return (
                <TableRow key={p.system}>
                  <TableCell className={FIRST}>{systemName(p.system)}</TableCell>
                  <TableCell colSpan={staged ? 5 : 6} className="min-w-56 whitespace-normal text-muted-foreground">
                    Load failed: {p.error?.split("\n")[0] ?? p.status}
                  </TableCell>
                </TableRow>
              )
            return (
              <TableRow key={p.system}>
                <TableCell className={cn(FIRST, p.system === "timelake" && "font-medium")}>
                  {systemName(p.system)}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {seconds(p.commit_seconds ?? p.seconds)}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {ms(staged ? p.stages?.live?.commit_ms_p50 : p.commit_ms_p50)}
                </TableCell>
                {!staged && <TableCell className="text-right font-mono tabular-nums">{ms(p.commit_ms_p95)}</TableCell>}
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
      <ScrollHint />
    </Panel>
  )
}

/** Fresh readers during ingest: how often they were behind, and dashboard queries under writes. */
function LivePanel({ run }: { run: Run }) {
  const rows = liveRows(run)
  if (rows.length === 0) return null
  return (
    <Panel
      title="Per system"
      note={
        <>
          Every {run.header.live_interval_s} s during ingest, a fresh reader ran the dashboard queries and checked the
          newest row it could see. Behind: probes that missed a commit that had already finished.
        </>
      }
      className="overflow-x-auto"
    >
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className={FIRST}>System</TableHead>
            <TableHead className="text-right">Probes behind</TableHead>
            <TableHead className="text-right">Most behind</TableHead>
            <TableHead className="text-right">Dashboard query, median</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((r) =>
            r.probes === 0 ? (
              <TableRow key={r.system}>
                <TableCell className={FIRST}>{systemName(r.system)}</TableCell>
                <TableCell colSpan={3} className="text-muted-foreground">
                  No probe finished during ingest.
                </TableCell>
              </TableRow>
            ) : (
              <TableRow key={r.system}>
                <TableCell className={cn(FIRST, r.system === "timelake" && "font-medium")}>
                  {systemName(r.system)}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {r.behind} of {r.probes}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {r.behind ? `${r.maxLag.toFixed(1)} s` : "—"}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {r.queryMedian == null ? "—" : formatTime(r.queryMedian)}
                </TableCell>
              </TableRow>
            ),
          )}
        </TableBody>
      </Table>
      <ScrollHint />
    </Panel>
  )
}

/** N dashboard clients at once: throughput and slow queries per client count. */
function ConcurrencyPanel({ run }: { run: Run }) {
  if (run.concurrency.length === 0) return null
  const counts = [...new Set(run.concurrency.map((c) => c.clients))].sort((a, b) => a - b)
  const systems = [...new Set(run.concurrency.map((c) => c.system))]
  return (
    <Panel
      title="Per system"
      note={
        <>
          After maintenance, {counts.join(", ")} clients at once, each with its own reader open, running the dashboard
          queries in turn for {run.concurrency[0]?.seconds} s. Queries per second over all clients, and the 95th
          percentile latency.
        </>
      }
      className="overflow-x-auto"
    >
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className={FIRST}>System</TableHead>
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
              <TableCell className={cn(FIRST, system === "timelake" && "font-medium")}>{systemName(system)}</TableCell>
              {counts.map((n) => {
                const c = run.concurrency.find((x) => x.system === system && x.clients === n)
                return (
                  <TableCell key={n} className="text-right font-mono whitespace-nowrap tabular-nums">
                    {c ? (
                      <>
                        <span className="block">{c.qps.toFixed(1)} q/s</span>
                        <span className="block text-xs text-muted-foreground">
                          p95 {c.p95_s == null ? "—" : formatTime(c.p95_s)}
                        </span>
                        {c.errors > 0 && (
                          <span className="block text-xs text-muted-foreground">
                            {c.errors} {c.errors === 1 ? "error" : "errors"}
                          </span>
                        )}
                      </>
                    ) : (
                      "—"
                    )}
                  </TableCell>
                )
              })}
            </TableRow>
          ))}
        </TableBody>
      </Table>
      <ScrollHint />
    </Panel>
  )
}

const usd = (x: number | null) => (x == null ? "—" : x < 0.01 ? `$${x.toFixed(4)}` : `$${x.toFixed(2)}`)

/** What the object-store traffic and storage cost at list prices. */
function CostPanel({ run }: { run: Run }) {
  const rows = costRows(run)
  if (rows.length === 0) return null
  return (
    <Panel
      title={`On ${targetLabel(run.header.target)}`}
      note="Ingest is scaled to a month of data arriving at this rate."
      className="overflow-x-auto"
    >
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className={FIRST}>System</TableHead>
            <TableHead className="text-right">Ingest, per month</TableHead>
            <TableHead className="text-right">Storage, per month</TableHead>
            <TableHead className="text-right">1M queries</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((r) => (
            <TableRow key={r.system}>
              <TableCell className={cn(FIRST, r.system === "timelake" && "font-medium")}>
                {systemName(r.system)}
              </TableCell>
              <TableCell className="text-right font-mono tabular-nums">{usd(r.ingestMonth)}</TableCell>
              <TableCell className="text-right font-mono tabular-nums">{usd(r.storageMonth)}</TableCell>
              <TableCell className="text-right font-mono tabular-nums">{usd(r.perMillionQueries)}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
      <ScrollHint />
    </Panel>
  )
}
