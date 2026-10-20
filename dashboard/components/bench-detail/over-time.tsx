import { BenchHistory } from "@/components/bench-history"
import { Panel } from "@/components/ranking-card"
import { RunInfoButton } from "@/components/run-info"
import { SectionHead } from "@/components/section-head"
import { systemColor } from "@/lib/bench-systems"
import {
  TUNING_LABELS,
  type Run,
  type Workload,
  engineLabel,
  history,
  runDate,
  runInfo,
  runLabel,
  systemList,
  targetLabel,
  tuningOf,
} from "@/lib/benchmarks"
import { REPO_URL } from "@/lib/site"
import { slugify } from "@/lib/utils"

export const OVER_TIME_TITLE = "Over time"

/** Each system's own reader over time on each kind of storage, and every run. */
export function OverTime({ runs, latest, workload }: { runs: Run[]; latest: Run[]; workload: Workload }) {
  const all = runs.filter((r) => r.header.workload === workload.id).toReversed()
  return (
    <section id={slugify(OVER_TIME_TITLE)} className="flex scroll-mt-20 flex-col gap-4">
      <SectionHead label={OVER_TIME_TITLE}>
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
                  lines={h.engines.map((e) => ({ engine: e, label: engineLabel(e), color: systemColor(e) }))}
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
              <span className="font-mono text-xs tabular-nums">{runDate(r)}</span>
              <span className="min-w-32">
                {runLabel(r)}
                {tuningOf(r) === "tuned" ? "" : ` · ${TUNING_LABELS[tuningOf(r)]?.toLowerCase() ?? tuningOf(r)}`}
              </span>
              <span className="min-w-0 flex-1 text-xs text-muted-foreground">{systemList(r)}</span>
              <span className="flex items-center gap-2">
                <a
                  className="font-mono text-xs text-muted-foreground hover:text-foreground hover:underline"
                  href={`${REPO_URL}/blob/main/bench/results/runs/${r.header.id}.jsonl`}
                >
                  {r.header.label === "weekly" ? "weekly" : "jsonl"}
                </a>
                <RunInfoButton info={runInfo(r)} label={`${runDate(r)} ${runLabel(r)}`} />
              </span>
            </li>
          ))}
        </ul>
      </Panel>
    </section>
  )
}
