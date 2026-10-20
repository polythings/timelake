import { ArrowUpRight } from "lucide-react"

import { RunInfoButton } from "@/components/run-info"
import { MoreLink } from "@/components/section-head"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import type { Benchmark } from "@/lib/benchmark-catalog"
import { type Run, runDate, runInfo, runKey, runLabel } from "@/lib/benchmarks"

/** Under a benchmark's summary: where its data and queries come from, and links to its pages and its original. */
export function BenchmarkLinks({ benchmark, results = false }: { benchmark: Benchmark; results?: boolean }) {
  return (
    <>
      {benchmark.provenance && (
        <p className="max-w-4xl text-xs leading-relaxed text-muted-foreground">{benchmark.provenance}</p>
      )}
      <nav className="flex flex-wrap gap-x-5 gap-y-1">
        {results && <MoreLink href={`/benchmarks/${benchmark.id}`}>All results</MoreLink>}
        <MoreLink href={`/benchmarks/${benchmark.id}/methodology`}>Methodology</MoreLink>
        {benchmark.origin && (
          <a href={benchmark.origin.url} className="flex w-fit items-center gap-1 text-sm hover:underline">
            {benchmark.origin.label} <ArrowUpRight className="size-3.5" />
          </a>
        )}
      </nav>
    </>
  )
}

/** A run and what a page shows of it. */
export type RunView = { run: Run; node: React.ReactNode }

/** A tab per kind of storage, named by its run's target and variant, each showing its run; the first is open. */
export function RunTabs({ views }: { views: RunView[] }) {
  const [first] = views
  if (!first) return null
  return (
    <Tabs defaultValue={runKey(first.run)} className="gap-4">
      <TabsList variant="line" className="flex-wrap" aria-label="Storage">
        {views.map((v) => (
          <TabsTrigger
            key={runKey(v.run)}
            value={runKey(v.run)}
            className="min-h-8 font-mono text-xs tracking-[0.06em] uppercase"
          >
            {runLabel(v.run)}
          </TabsTrigger>
        ))}
      </TabsList>
      {views.map((v) => (
        <TabsContent key={runKey(v.run)} value={runKey(v.run)} className="flex flex-col gap-4">
          {v.node}
        </TabsContent>
      ))}
    </Tabs>
  )
}

/** "Run of 2026-10-19", and the run's details behind ⓘ. */
export function RunDate({ run, label = runLabel(run) }: { run: Run; label?: string }) {
  return (
    <>
      Run of {runDate(run)}
      <RunInfoButton info={runInfo(run)} label={label} />
    </>
  )
}
