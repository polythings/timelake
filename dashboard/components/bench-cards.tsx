import { Race, type Racer } from "@/components/race"
import { RankingCard } from "@/components/ranking-card"
import { BENCHMARKS } from "@/lib/benchmark-catalog"
import { type Run, SYSTEM_COLORS, engineLabel, rankings } from "@/lib/benchmarks"

/** Each system's own reader for the request race, then the others through DuckDB. */
const RACE_ENGINES = ["timelake+timelake", "lance+duckdb", "iceberg+duckdb", "parquet+duckdb"]

/**
 * A run as cards: query times after maintenance and right after ingest (unless `queries` is false, where a page shows
 * them by suite), commit or load times, and the requests one query takes on each system.
 */
export function BenchCards({ run, queries = true }: { run: Run; queries?: boolean }) {
  // Entries' "learn more" goes to the list of entries on the benchmark's page.
  const id = BENCHMARKS.find((b) => b.workload === run.header.workload)?.id
  const page = id ? `/benchmarks/${id}` : undefined
  const racers: Racer[] = RACE_ENGINES.flatMap((e) => {
    const q = run.queries.find(
      (x) =>
        `${x.system}+${x.reader}` === e && x.query === "last-loc" && x.checkpoint === "ingested" && x.status === "ok",
    )
    const n = q?.requests?.[0]
    if (!q || n === undefined) return []
    const system = e.split("+")[0] ?? ""
    return [
      {
        label: engineLabel(e),
        value: n,
        dots: n,
        seconds: q.times[0],
        timelake: system === "timelake",
        color: SYSTEM_COLORS[system] ?? "var(--chart-2)",
      },
    ]
  })
  const trucks = run.queries.find((q) => q.query === "last-loc" && q.system === "timelake")?.rows
  return (
    <div className="grid gap-4 lg:grid-cols-2 lg:items-start">
      {rankings(run)
        .filter((r) => queries || r.queries === undefined)
        .map((r) => (
          <RankingCard key={r.title} ranking={r} learnMore={page} />
        ))}
      {racers.length > 1 ? (
        <Race
          title={`Latest position of ${trucks?.toLocaleString("en") ?? "every"} trucks`}
          racers={racers}
          unit="requests"
          dot={2}
          caption="Requests each system makes for one query, right after ingest, and the time it takes; each dot is a request."
        />
      ) : null}
    </div>
  )
}
