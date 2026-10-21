import { NumberCell, SystemRow, SystemTable } from "@/components/bench-detail/system-table"
import type { Run } from "@/lib/benchmarks"
import { formatNumber, formatTime, orDash } from "@/lib/format"

const clients = (n: number) => `${n} ${n === 1 ? "client" : "clients"}`

/** N dashboard clients at once: throughput and slow queries per client count. */
export function ConcurrencyPanel({ run }: { run: Run }) {
  if (run.concurrency.length === 0) return null
  const counts = [...new Set(run.concurrency.map((c) => c.clients))].sort((a, b) => a - b)
  const systems = [...new Set(run.concurrency.map((c) => c.system))]
  return (
    <SystemTable
      title="Per system"
      note={
        <>
          After maintenance, {counts.join(", ")} clients at once, each with its own reader open, running the dashboard
          queries in turn for {run.concurrency[0]?.seconds} s. Queries per second over all clients, and the 95th
          percentile latency.
        </>
      }
      columns={counts.map(clients)}
    >
      {systems.map((system) => (
        <SystemRow key={system} system={system} systems={run.header.systems}>
          {counts.map((n) => {
            const c = run.concurrency.find((x) => x.system === system && x.clients === n)
            return (
              <NumberCell key={n} className="whitespace-nowrap">
                {c ? (
                  <>
                    <span className="block">{formatNumber(c.qps, 1)} q/s</span>
                    <span className="block text-xs text-muted-foreground">p95 {orDash(c.p95_s, formatTime)}</span>
                    {c.errors > 0 && (
                      <span className="block text-xs text-muted-foreground">
                        {c.errors} {c.errors === 1 ? "error" : "errors"}
                      </span>
                    )}
                  </>
                ) : (
                  "—"
                )}
              </NumberCell>
            )
          })}
        </SystemRow>
      ))}
    </SystemTable>
  )
}
