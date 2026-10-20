import { Panel } from "@/components/ranking-card"
import { SYSTEM_INFO, systemLabel } from "@/lib/bench-systems"
import type { PhaseRecord, Run } from "@/lib/benchmarks"
import { formatDuration, orDash } from "@/lib/format"

/** How long a system's step after ingest took: "managed" where the store runs it, a dash where it has none. */
function stepTime(p: PhaseRecord): string {
  const maintains = SYSTEM_INFO[p.system]?.maintains
  if (maintains === "managed") return "managed"
  if (maintains === "none") return "—"
  return orDash(p.seconds, formatDuration)
}

/** When each system maintained its tables before the queries after maintenance, and how long its step after ingest took. */
export function MaintenancePanel({ run }: { run: Run }) {
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
              {systemLabel(p.system)}
            </span>
            <span className="text-xs text-muted-foreground">{SYSTEM_INFO[p.system]?.when}</span>
            <span className="font-mono text-xs tabular-nums sm:text-right">{stepTime(p)}</span>
          </li>
        ))}
      </ul>
    </Panel>
  )
}
