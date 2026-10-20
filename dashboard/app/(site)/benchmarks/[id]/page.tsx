import type { Metadata } from "next"
import { notFound } from "next/navigation"
import { Suspense } from "react"

import { BackLink } from "@/components/back-link"
import { ConcurrencyPanel } from "@/components/bench-detail/concurrency-panel"
import { CostPanel } from "@/components/bench-detail/cost-panel"
import { ENTRIES_TITLE, EntriesSection } from "@/components/bench-detail/entries-section"
import { FailedPanel } from "@/components/bench-detail/failed-panel"
import { IngestContent } from "@/components/bench-detail/ingest"
import { LivePanel } from "@/components/bench-detail/live-panel"
import { MaintenancePanel } from "@/components/bench-detail/maintenance-panel"
import { OVER_TIME_TITLE, OverTime } from "@/components/bench-detail/over-time"
import { ResultSection } from "@/components/bench-detail/result-section"
import { BenchmarkLinks, type RunView } from "@/components/bench-run"
import { SectionResults } from "@/components/bench-sections"
import { PageTitle } from "@/components/page-title"
import { BENCHMARKS, benchmark } from "@/lib/benchmark-catalog"
import {
  CHECKED_NOTE,
  CHECKPOINTS,
  CHECKPOINT_NOTES,
  WORKLOADS,
  answersChecked,
  costRows,
  failures,
  latestByTarget,
  liveRows,
  loadRuns,
  sectionHistory,
  sections,
  type Section,
  type Workload,
} from "@/lib/benchmarks"
import { slugify } from "@/lib/utils"

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
        <BackLink href="/benchmarks">Benchmarks</BackLink>
        <PageTitle>{b.name}</PageTitle>
        <p className="max-w-4xl text-sm text-muted-foreground">{b.summary}</p>
        <BenchmarkLinks benchmark={b} />
      </header>

      <Results workload={workload} />
    </>
  )
}

async function Results({ workload }: { workload: Workload }) {
  const runs = await loadRuns()
  const latest = latestByTarget(runs, workload.id)
  if (latest.length === 0) return <p className="text-sm text-muted-foreground">No runs yet.</p>
  const checked = latest.some(answersChecked)
  // The query sections of every storage, each with the runs that have it, in suite then checkpoint order.
  const order = (s: Section) =>
    workload.suites.findIndex((x) => x.id === s.suite) * 100 + Object.keys(CHECKPOINTS).indexOf(s.checkpoint)
  const querySections = new Map<string, { section: Section; views: RunView[] }>()
  for (const r of latest) {
    for (const s of sections(r, workload)) {
      const key = `${s.suite}|${s.checkpoint}`
      const entry = querySections.get(key) ?? { section: s, views: [] }
      const results = <SectionResults section={s} history={sectionHistory(runs, r, s.suite, s.checkpoint)} />
      entry.views.push({
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
    ENTRIES_TITLE,
    OVER_TIME_TITLE,
  ]
  return (
    <>
      <nav aria-label="On this page" className="-mt-8 flex flex-wrap gap-x-1 gap-y-1 text-sm">
        {jumps.map((j) => (
          <a
            key={j}
            href={`#${slugify(j)}`}
            className="inline-flex min-h-8 items-center rounded-md border px-2.5 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
          >
            {j}
          </a>
        ))}
      </nav>
      {checked && (
        <p className="-mt-8 text-sm text-muted-foreground">
          {CHECKED_NOTE} Each section shows the newest run on the storage you pick.
        </p>
      )}
      {sorted.map(({ section: s, views }) => (
        <ResultSection
          key={`${s.suite}|${s.checkpoint}`}
          title={queryTitle(s)}
          note={CHECKPOINT_NOTES[s.checkpoint]}
          views={views}
        />
      ))}
      <ResultSection
        title="Ingest and maintenance"
        note="How each system took the data in and kept it in shape: load rate, commit times, maintenance, and what it stored."
        views={ingest.map((r) => ({ run: r, node: <IngestContent run={r} /> }))}
      />
      <ResultSection
        title="While ingesting"
        note="Fresh readers during ingest: whether they saw every finished commit, and how fast dashboard queries ran under writes."
        views={live.map((r) => ({ run: r, node: <LivePanel run={r} /> }))}
      />
      <ResultSection
        title="Concurrent dashboards"
        note="Several clients at once after maintenance, each with its own reader, running the dashboard queries in turn."
        views={concurrent.map((r) => ({ run: r, node: <ConcurrencyPanel run={r} /> }))}
      />
      <ResultSection
        title="Cost"
        note="Requests and storage at the store's list prices; machines are not included."
        views={costs.map((r) => ({ run: r, node: <CostPanel run={r} /> }))}
      />
      <ResultSection
        title="Failed or timed out"
        note="Left out of the rankings above; each query's list says which readers it lost."
        views={failing.map((r) => ({ run: r, node: <FailedPanel failed={failures(r)} /> }))}
      />
      <EntriesSection latest={latest} />
      <OverTime runs={runs} latest={latest} workload={workload} />
    </>
  )
}
