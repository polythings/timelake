import { Race } from "@/components/race"
import { RankingCard } from "@/components/ranking-card"
import { BENCHMARKS } from "@/lib/benchmark-catalog"
import { type Run, lastLocRacers, lastLocRows, rankings } from "@/lib/benchmarks"
import { formatNumber } from "@/lib/format"

/**
 * A run as cards: query times after maintenance and right after ingest (unless `queries` is false, where a page shows
 * them by suite), commit or load times, and the requests one query takes on each system.
 */
export function BenchCards({ run, queries = true }: { run: Run; queries?: boolean }) {
  // Entries' "learn more" goes to the list of entries on the benchmark's page.
  const id = BENCHMARKS.find((b) => b.workload === run.header.workload)?.id
  const page = id ? `/benchmarks/${id}` : undefined
  const racers = lastLocRacers(run)
  const trucks = lastLocRows(run)
  return (
    <div className="grid gap-4 lg:grid-cols-2 lg:items-start">
      {rankings(run)
        .filter((r) => queries || r.queries === undefined)
        .map((r) => (
          <RankingCard key={r.id} ranking={r} learnMore={page} />
        ))}
      {racers.length > 1 ? (
        <Race
          title={`Latest position of ${trucks === undefined ? "every" : formatNumber(trucks)} trucks`}
          racers={racers}
          unit="requests"
          dot={2}
          caption="Requests each system makes for one query, right after ingest, and the time it takes; each dot is a request."
        />
      ) : null}
    </div>
  )
}
