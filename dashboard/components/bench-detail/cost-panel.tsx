import { NumberCell, SystemRow, SystemTable } from "@/components/bench-detail/system-table"
import { type Run, costRows, targetLabel } from "@/lib/benchmarks"
import { formatUsd, orDash } from "@/lib/format"

/** What the object-store traffic and storage cost at list prices. */
export function CostPanel({ run }: { run: Run }) {
  const rows = costRows(run)
  if (rows.length === 0) return null
  return (
    <SystemTable
      title={`On ${targetLabel(run.header.target)}`}
      note="Ingest is scaled to a month of data arriving at this rate."
      columns={["Ingest, per month", "Storage, per month", "Per query"]}
    >
      {rows.map((r) => (
        <SystemRow key={r.system} system={r.system} systems={run.header.systems}>
          <NumberCell>{orDash(r.ingestMonth, formatUsd)}</NumberCell>
          <NumberCell>{orDash(r.storageMonth, formatUsd)}</NumberCell>
          <NumberCell>{orDash(r.perQuery, formatUsd)}</NumberCell>
        </SystemRow>
      ))}
    </SystemTable>
  )
}
