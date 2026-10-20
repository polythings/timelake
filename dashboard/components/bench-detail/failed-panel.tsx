import { Panel } from "@/components/ranking-card"
import { CHECKPOINTS, type Failure } from "@/lib/benchmarks"

/** Queries that failed or timed out, by reader: they're left out of the rankings. */
export function FailedPanel({ failed }: { failed: Failure[] }) {
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
