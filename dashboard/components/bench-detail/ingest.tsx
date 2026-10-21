import { BenchCards } from "@/components/bench-cards"
import { NumberCell, SystemRow, SystemTable } from "@/components/bench-detail/system-table"
import { TableCell } from "@/components/ui/table"
import { SYSTEM_INFO } from "@/lib/bench-systems"
import { type Run, s3TablesDelay } from "@/lib/benchmarks"
import { formatBytes, formatDuration, formatMs, formatNumber, orDash } from "@/lib/format"

/** A run's ingest: load and commit rankings, the requests one query takes, and the per-system table. */
export function IngestContent({ run }: { run: Run }) {
  const delay = s3TablesDelay(run)
  return (
    <>
      <BenchCards run={run} queries={false} />
      <IngestPanel run={run} />
      {delay && (
        <p className="text-sm text-muted-foreground">
          S3 Tables compacted {formatNumber(delay[0], 1)}–{formatNumber(delay[1], 1)} hours after ingest; until then its
          readers see the &quot;right after ingest&quot; numbers.
        </p>
      )}
    </>
  )
}

/** Each system's ingest and maintenance: time committing, commit latency, files and storage after maintenance. */
function IngestPanel({ run }: { run: Run }) {
  const ingest = run.phases.filter((p) => p.kind === "ingest")
  if (ingest.length === 0) return null
  const maintain = new Map(run.phases.filter((p) => p.kind === "maintain").map((p) => [p.system, p]))
  // A staged stream (history in bulk, then a live day): commit times are the live day's.
  const staged = ingest.some((p) => p.stages)
  return (
    <SystemTable
      title="Per system"
      note={
        <>
          Committing is the time spent in all commits together
          {staged ? "; commit times are the live day's, a 5-minute window per commit" : ""}. Storage is all files after
          maintenance, counting replaced files still waiting for cleanup.
        </>
      }
      columns={["Committing", "Commit p50", ...(staged ? [] : ["Commit p95"]), "Maintenance", "Files", "Storage"]}
    >
      {ingest.map((p) => {
        const m = maintain.get(p.system)
        const after = m ?? p
        if (p.status !== "ok")
          return (
            <SystemRow key={p.system} system={p.system} systems={run.header.systems} plain>
              <TableCell colSpan={staged ? 5 : 6} className="min-w-56 whitespace-normal text-muted-foreground">
                Load failed: {p.error?.split("\n")[0] ?? p.status}
              </TableCell>
            </SystemRow>
          )
        return (
          <SystemRow key={p.system} system={p.system} systems={run.header.systems}>
            <NumberCell>{orDash(p.commit_seconds ?? p.seconds, formatDuration)}</NumberCell>
            <NumberCell>{orDash(staged ? p.stages?.live?.commit_ms_p50 : p.commit_ms_p50, formatMs)}</NumberCell>
            {!staged && <NumberCell>{orDash(p.commit_ms_p95, formatMs)}</NumberCell>}
            <NumberCell>
              {SYSTEM_INFO[p.system]?.maintains === "managed" ? "managed" : orDash(m?.seconds, formatDuration)}
            </NumberCell>
            <NumberCell>{orDash(after.data_files, formatNumber)}</NumberCell>
            <NumberCell>{orDash(after.bytes, formatBytes)}</NumberCell>
          </SystemRow>
        )
      })}
    </SystemTable>
  )
}
