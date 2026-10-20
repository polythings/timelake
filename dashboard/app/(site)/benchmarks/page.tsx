import {
  BotMessageSquare,
  Factory,
  FlaskConical,
  Hourglass,
  type LucideIcon,
  MousePointerClick,
  RadioTower,
  Receipt,
  Thermometer,
} from "lucide-react"
import type { Metadata } from "next"
import Link from "next/link"
import { Suspense } from "react"

import { BenchmarkLinks, RunDate, RunTabs } from "@/components/bench-run"
import { PageTitle } from "@/components/page-title"
import { RankingCard } from "@/components/ranking-card"
import { MoreLink, SectionHead } from "@/components/section-head"
import { BENCHMARKS, type Benchmark } from "@/lib/benchmark-catalog"
import {
  CHECKED_NOTE,
  answersChecked,
  latestByTarget,
  loadRuns,
  rankings,
  systemList,
  tuningOf,
  type Run,
} from "@/lib/benchmarks"

export const metadata: Metadata = { title: "Benchmarks" }

/** An icon per planned benchmark, for what its data is. */
const ICONS: Record<string, LucideIcon> = {
  clickbench: MousePointerClick,
  rtabench: Receipt,
  "tsm-bench": Thermometer,
  "iot-benchmark": RadioTower,
  longevity: Hourglass,
  "industrial-iot": Factory,
  "ai-agents": BotMessageSquare,
}

const RUNNING = BENCHMARKS.filter((b) => b.status === "running" && b.workload)
const PLANNED = BENCHMARKS.filter((b) => b.status === "planned")

const ITEM =
  "block truncate rounded-md px-2 py-1.5 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"

/** The benchmarks we run, as links to their results on this page, then the ones coming next and how we run them. */
function BenchmarksNav() {
  return (
    <nav className="space-y-6 text-sm" aria-label="Benchmarks">
      <div className="space-y-1">
        <div className="label-mono px-2">Running</div>
        {RUNNING.map((b) => (
          <a key={b.id} href={`#${b.id}`} className={ITEM}>
            {b.name}
          </a>
        ))}
      </div>
      <div className="space-y-1">
        <div className="label-mono px-2">Coming next</div>
        {PLANNED.map((b) => (
          <Link key={b.id} href={`/benchmarks/${b.id}/methodology`} className={ITEM}>
            {b.name}
          </Link>
        ))}
      </div>
      <div className="space-y-1">
        <div className="label-mono px-2">Method</div>
        <Link href="/benchmarks/timebench" className={ITEM}>
          timebench
        </Link>
      </div>
    </nav>
  )
}

export default function BenchmarksPage() {
  return (
    <div className="mx-auto flex max-w-6xl gap-10 px-4 pt-8 pb-24">
      <aside className="sticky top-20 hidden h-[calc(100svh-6rem)] w-52 shrink-0 overflow-y-auto md:block">
        <BenchmarksNav />
      </aside>
      <div className="flex min-w-0 flex-1 flex-col gap-16">
        <PageTitle>Benchmarks</PageTitle>

        <Suspense fallback={<div className="h-96 animate-pulse rounded-lg bg-muted" />}>
          <Results />
        </Suspense>

        <section id="coming-next" className="flex scroll-mt-20 flex-col gap-4">
          <SectionHead label="Coming next">
            The benchmarks we plan to run next. The ones we decided not to run, and why, are on the{" "}
            <Link
              href="/benchmarks/timebench#benchmarks"
              className="underline underline-offset-4 hover:text-foreground"
            >
              timebench page
            </Link>
            .
          </SectionHead>
          <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
            {PLANNED.map((b) => {
              const Icon = ICONS[b.id] ?? FlaskConical
              return (
                <Link
                  key={b.id}
                  href={`/benchmarks/${b.id}/methodology`}
                  className="flex flex-col gap-2 rounded-lg border border-dashed p-6 transition-colors hover:bg-card"
                >
                  <h3 className="flex items-center gap-2.5 text-lg font-normal">
                    <Icon className="size-4.5 shrink-0 text-muted-foreground" aria-hidden />
                    {b.name}
                  </h3>
                  <p className="text-sm leading-relaxed text-muted-foreground">{b.summary}</p>
                </Link>
              )
            })}
          </div>
        </section>

        <MoreLink href="/benchmarks/timebench">How we run every benchmark: timebench</MoreLink>
      </div>
    </div>
  )
}

async function Results() {
  const runs = await loadRuns()
  return RUNNING.map((b) => (
    <BenchmarkResults
      key={b.id}
      benchmark={b}
      // The tuned runs here; a benchmark's page switches between tunings.
      latest={latestByTarget(runs, b.workload ?? "").filter((r) => tuningOf(r) === "tuned")}
    />
  ))
}

/**
 * A benchmark's newest run on each kind of storage, a tab each, with its headline result: query times after
 * maintenance. Its page has the rest.
 */
function BenchmarkResults({ benchmark, latest }: { benchmark: Benchmark; latest: Run[] }) {
  return (
    <section id={benchmark.id} className="flex scroll-mt-20 flex-col gap-4">
      <div className="flex flex-col gap-3">
        <SectionHead label={benchmark.name}>{benchmark.summary}</SectionHead>
        <BenchmarkLinks benchmark={benchmark} results />
      </div>
      {latest.length > 0 ? (
        <RunTabs views={latest.map((r) => ({ run: r, node: <LatestRun benchmark={benchmark} run={r} /> }))} />
      ) : (
        <p className="text-sm text-muted-foreground">No runs yet.</p>
      )}
    </section>
  )
}

/** One storage's newest run: its headline ranking, or what ran where nothing is ranked; then its date. */
function LatestRun({ benchmark, run }: { benchmark: Benchmark; run: Run }) {
  const page = `/benchmarks/${benchmark.id}`
  const [headline] = rankings(run)
  return (
    <>
      {headline ? (
        <RankingCard ranking={headline} learnMore={page} />
      ) : (
        <p className="text-sm text-muted-foreground">
          Only {systemList(run)} ran here, so nothing is ranked. Its times are under{" "}
          <Link href={page} className="underline underline-offset-4 hover:text-foreground">
            all results
          </Link>
          .
        </p>
      )}
      <p className="flex flex-wrap items-center gap-x-1.5 text-xs text-muted-foreground">
        <RunDate run={run} />
        {answersChecked(run) && CHECKED_NOTE}
      </p>
    </>
  )
}
