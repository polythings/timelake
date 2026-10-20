import {
  ArrowUpRight,
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

import { BenchCards } from "@/components/bench-cards"
import { RunInfoButton } from "@/components/run-info"
import { MoreLink, SectionHead } from "@/components/section-head"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { BENCHMARKS, type Benchmark } from "@/lib/benchmark-catalog"
import { SYSTEMS, latestByTarget, loadRuns, rankings, runInfo, runKey, runLabel, type Run } from "@/lib/benchmarks"

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

export default function BenchmarksPage() {
  const planned = BENCHMARKS.filter((b) => b.status === "planned")
  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-16 px-4 pt-8 pb-24">
      <h1 className="text-4xl leading-[1.05] font-light tracking-tight sm:text-5xl">Benchmarks</h1>

      <Suspense fallback={<div className="h-96 animate-pulse rounded-lg bg-muted" />}>
        <Results />
      </Suspense>

      <section className="flex flex-col gap-4">
        <SectionHead label="Coming next">
          The benchmarks we decided not to run, and why, are on the{" "}
          <Link href="/benchmarks/timebench#benchmarks" className="underline underline-offset-4 hover:text-foreground">
            timebench page
          </Link>
          .
        </SectionHead>
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {planned.map((b) => {
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
  )
}

async function Results() {
  const runs = await loadRuns()
  return BENCHMARKS.filter((b) => b.status === "running" && b.workload).map((b) => (
    <BenchmarkResults key={b.id} benchmark={b} latest={latestByTarget(runs, b.workload ?? "")} />
  ))
}

/** A benchmark's newest run on each kind of storage, a tab each. */
function BenchmarkResults({ benchmark, latest }: { benchmark: Benchmark; latest: Run[] }) {
  const [first] = latest
  return (
    <section className="flex flex-col gap-4">
      <div className="flex flex-col gap-3">
        <SectionHead label={benchmark.name}>{benchmark.summary}</SectionHead>
        {benchmark.provenance && (
          <p className="max-w-4xl text-xs leading-relaxed text-muted-foreground">{benchmark.provenance}</p>
        )}
        <nav className="flex flex-wrap gap-x-5 gap-y-1">
          <MoreLink href={`/benchmarks/${benchmark.id}`}>All results</MoreLink>
          <MoreLink href={`/benchmarks/${benchmark.id}/methodology`}>Methodology</MoreLink>
          {benchmark.origin && (
            <a href={benchmark.origin.url} className="flex w-fit items-center gap-1 text-sm hover:underline">
              {benchmark.origin.label} <ArrowUpRight className="size-3.5" />
            </a>
          )}
        </nav>
      </div>
      {first ? (
        <Tabs defaultValue={runKey(first)} className="gap-4">
          <TabsList variant="line" className="flex-wrap">
            {latest.map((r) => (
              <TabsTrigger key={runKey(r)} value={runKey(r)} className="font-mono text-xs tracking-[0.06em] uppercase">
                {runLabel(r)}
              </TabsTrigger>
            ))}
          </TabsList>
          {latest.map((r) => (
            <TabsContent key={runKey(r)} value={runKey(r)} className="flex flex-col gap-4">
              {rankings(r).length > 0 ? (
                <BenchCards run={r} />
              ) : (
                <p className="text-sm text-muted-foreground">
                  Only{" "}
                  {Object.keys(r.header.readers)
                    .map((s) => SYSTEMS[s] ?? s)
                    .join(", ")}{" "}
                  ran here, so nothing is ranked. Its times are under{" "}
                  <Link
                    href={`/benchmarks/${benchmark.id}`}
                    className="underline underline-offset-4 hover:text-foreground"
                  >
                    all results
                  </Link>
                  .
                </p>
              )}
              <p className="flex flex-wrap items-center gap-x-1.5 text-xs text-muted-foreground">
                Run of {r.header.started.slice(0, 10)}
                <RunInfoButton info={runInfo(r)} label={runLabel(r)} />
                {r.queries.some((q) => q.correct !== undefined) &&
                  "Answers are checked against a copy of the data with each row once."}
              </p>
            </TabsContent>
          ))}
        </Tabs>
      ) : (
        <p className="text-sm text-muted-foreground">No runs yet.</p>
      )}
    </section>
  )
}
