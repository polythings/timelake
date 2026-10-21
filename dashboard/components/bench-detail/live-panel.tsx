import { NumberCell, SystemRow, SystemTable } from "@/components/bench-detail/system-table"
import { TableCell } from "@/components/ui/table"
import { type Run, liveRows } from "@/lib/benchmarks"
import { formatNumber, formatTime, orDash } from "@/lib/format"

/** Fresh readers during ingest: how often they were behind, and dashboard queries under writes. */
export function LivePanel({ run }: { run: Run }) {
  const rows = liveRows(run)
  if (rows.length === 0) return null
  return (
    <SystemTable
      title="Per system"
      note={
        <>
          Every {run.header.live_interval_s} s during ingest, a fresh reader ran the dashboard queries and checked the
          newest row it could see. Behind: probes that missed a commit that had already finished.
        </>
      }
      columns={["Probes behind", "Most behind", "Dashboard query, median"]}
    >
      {rows.map((r) =>
        r.probes === 0 ? (
          <SystemRow key={r.system} system={r.system} systems={run.header.systems} plain>
            <TableCell colSpan={3} className="text-muted-foreground">
              No probe finished during ingest.
            </TableCell>
          </SystemRow>
        ) : (
          <SystemRow key={r.system} system={r.system} systems={run.header.systems}>
            <NumberCell>
              {r.behind} of {r.probes}
            </NumberCell>
            <NumberCell>{r.behind ? `${formatNumber(r.maxLag, 1)} s` : "—"}</NumberCell>
            <NumberCell>{orDash(r.queryMedian, formatTime)}</NumberCell>
          </SystemRow>
        ),
      )}
    </SystemTable>
  )
}
