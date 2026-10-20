import { ArrowUpRight, ChevronLeft } from "lucide-react"
import type { Metadata } from "next"
import Link from "next/link"

import { Badge } from "@/components/ui/badge"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { READERS, SYSTEMS, SYSTEM_INFO } from "@/lib/bench-systems"
import { BENCHMARKS, type Status } from "@/lib/benchmark-catalog"
import { REPO_URL } from "@/lib/site"

export const metadata: Metadata = {
  title: "timebench",
  description: "How every benchmark on this site is run: workloads, systems, measurements, and history.",
}

const STATUS: Record<Status, { label: string; variant: "default" | "secondary" | "outline" }> = {
  running: { label: "Running", variant: "default" },
  planned: { label: "Planned", variant: "secondary" },
  "not planned": { label: "Not planned", variant: "outline" },
}

function H2({ id, children }: { id?: string; children: React.ReactNode }) {
  return (
    <h2 id={id} className="scroll-mt-20 text-xl font-semibold tracking-tight">
      {children}
    </h2>
  )
}

export default function TimebenchPage() {
  return (
    <div className="mx-auto max-w-3xl space-y-10 px-4 py-12">
      <header className="space-y-3">
        <Link
          href="/benchmarks"
          className="flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          <ChevronLeft className="size-4" /> Benchmarks
        </Link>
        <h1 className="text-3xl font-semibold tracking-tight">timebench</h1>
        <p className="text-muted-foreground">
          The benchmark system behind every result on this site. It streams the same data into each system, runs each
          system&apos;s own maintenance, and times the same queries before and after. It is open source, lives in the{" "}
          <a className="underline underline-offset-4" href={`${REPO_URL}/tree/main/bench`}>
            Timelake repository
          </a>{" "}
          for now, and is built to move to a repository of its own.
        </p>
      </header>

      <section className="space-y-3">
        <H2>Principles</H2>
        <ul className="ml-5 list-disc space-y-1.5 text-muted-foreground">
          <li>Every system gets the same rows, in the same order, and the same queries.</li>
          <li>Each system is written, maintained, and read the way its own documentation suggests.</li>
          <li>Data arrives as it does in production, in small batches from a stream, not in one bulk load.</li>
          <li>
            Not only table formats: databases and plain files run too, so a result says what a team would otherwise use.
          </li>
          <li>Losses are published next to wins.</li>
          <li>Every run is kept, with the commit, the machine, and every version, and anyone can rerun it.</li>
        </ul>
      </section>

      <section className="space-y-3">
        <H2>How a run works</H2>
        <ol className="ml-5 list-decimal space-y-1.5 text-muted-foreground">
          <li>The workload&apos;s data is generated from a fixed seed, the same every time.</li>
          <li>
            <span className="text-foreground">Ingest.</span> Each system creates its tables and commits the data batch
            by batch. Each commit is timed, and the files and requests it took are counted.
          </li>
          <li>
            <span className="text-foreground">Probes while ingesting.</span> Every few seconds, a fresh reader runs the
            dashboard queries and asks for the newest row it can see. That gives query latency while data is being
            written, and whether a reader sees every commit that has finished.
          </li>
          <li>
            <span className="text-foreground">Queries right after ingest.</span> What readers see before any cleanup:
            many small files and a long history of commits. Systems whose compaction runs later, as a scheduled job or a
            managed service, look like this until it has run.
          </li>
          <li>
            <span className="text-foreground">Maintenance.</span> Each system&apos;s own: compaction, cleanup of old
            versions. For S3 Tables, the run waits for AWS&apos;s managed compaction, which takes hours.
          </li>
          <li>
            <span className="text-foreground">Queries after maintenance.</span> The same queries on tidy tables, the
            best case for each system.
          </li>
          <li>
            <span className="text-foreground">Concurrent dashboards.</span> 1, 4, and then 16 clients at once, each
            keeping a reader open like a dashboard backend, run the dashboard queries in turn for 30 seconds: queries
            per second over all clients, and how slow the slowest 5% were.
          </li>
          <li>Row counts are compared across systems. A mismatch is recorded with the run.</li>
        </ol>
      </section>

      <section className="space-y-3">
        <H2>Reading the results</H2>
        <ul className="ml-5 list-disc space-y-1.5 text-muted-foreground">
          <li>
            <span className="text-foreground">First run.</span> Each query runs in a fresh process, once, then twice
            more. The first run loads table metadata, file footers, and data from storage: the latency a new dashboard,
            job, or serverless function sees. The ranked lists show first runs; the results pages also show the best
            repeat.
          </li>
          <li>
            <span className="text-foreground">Geometric mean.</span> A benchmark&apos;s time is the geometric mean over
            the queries every system completed, so one slow query doesn&apos;t hide the rest and every system answers
            the same set. The multiplier is relative to the fastest.
          </li>
          <li>
            <span className="text-foreground">System and reader.</span> &quot;Lance + DuckDB&quot; means Lance&apos;s
            tables read by DuckDB. Timelake alone means Timelake&apos;s own reader.
          </li>
          <li>
            <span className="text-foreground">Failures</span> are left out of the means and listed on the results page.
            A query that runs past its time limit isn&apos;t run again at that system&apos;s later checkpoints; it is
            listed as skipped.
          </li>
          <li>
            <span className="text-foreground">Requests and bytes.</span> On S3, every request goes through a proxy that
            counts requests and bytes: what object storage bills for, and the same wherever the client runs.
          </li>
          <li>
            <span className="text-foreground">Cost.</span> On AWS, requests by kind and the bytes stored are priced at
            S3&apos;s list prices for the region, recorded with each run so later price changes don&apos;t rewrite
            history. Ingest is scaled to a month of data arriving at the benchmark&apos;s rate. Machines aren&apos;t
            included yet, and S3 Tables&apos; own storage and compaction fees aren&apos;t modeled.
          </li>
          <li>
            <span className="text-foreground">Database servers</span> keep data in memory between queries, so their
            first run is warmer than a fresh process reading object storage.
          </li>
        </ul>
      </section>

      <section className="space-y-3">
        <H2 id="systems">Systems</H2>
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>System</TableHead>
                <TableHead>What it is</TableHead>
                <TableHead>Written</TableHead>
                <TableHead>Maintenance</TableHead>
                <TableHead>Read by</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {Object.entries(SYSTEM_INFO).map(([system, info]) => (
                <TableRow key={system}>
                  <TableCell className="font-medium whitespace-normal">{SYSTEMS[system] ?? system}</TableCell>
                  <TableCell className="min-w-56 whitespace-normal text-muted-foreground">{info.summary}</TableCell>
                  <TableCell className="min-w-48 whitespace-normal text-muted-foreground">{info.writes}</TableCell>
                  <TableCell className="min-w-48 whitespace-normal text-muted-foreground">{info.maintenance}</TableCell>
                  <TableCell className="text-muted-foreground">
                    {info.readers.map((r) => READERS[r] ?? r).join(", ")}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
        <p className="text-muted-foreground">
          Each benchmark&apos;s page lists the systems in its results, and its methodology what each does in it.
          Planned: Iceberg written the way an expert team would, with a streaming sink and scheduled compaction.
        </p>
      </section>

      <section className="space-y-3">
        <H2>Where it runs, and the history</H2>
        <ul className="ml-5 list-disc space-y-1.5 text-muted-foreground">
          <li>
            Tables live on the machine&apos;s disk, or on S3. For development, a local object store with a fixed delay
            per request stands in for S3.
          </li>
          <li>
            Every week, the same benchmarks run on one fixed AWS machine in us-east-2 (8 Graviton cores, local SSD):
            TSBS on Mondays, on the disk and on S3 in the same region, and industrial IoT on Thursdays. Other
            systems&apos; versions are pinned, so a change in the history comes from Timelake.
          </li>
          <li>
            Each run is one file in the repository, which the results pages chart over time. Each results page links the
            commit it measured.
          </li>
        </ul>
      </section>

      <section className="space-y-3">
        <H2 id="benchmarks">Benchmarks</H2>
        <p className="text-muted-foreground">
          The public benchmarks for time-series data that we know of, our own, and why each runs or doesn&apos;t.
        </p>
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Benchmark</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Why</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {BENCHMARKS.map((b) => (
                <TableRow key={b.id}>
                  <TableCell className="align-top font-medium whitespace-nowrap">
                    {b.methodology ? (
                      <Link href={`/benchmarks/${b.id}/methodology`} className="hover:underline">
                        {b.name}
                      </Link>
                    ) : b.origin ? (
                      <a href={b.origin.url} className="flex items-center gap-1 hover:underline">
                        {b.name} <ArrowUpRight className="size-3.5 text-muted-foreground" />
                      </a>
                    ) : (
                      b.name
                    )}
                  </TableCell>
                  <TableCell className="align-top">
                    <Badge variant={STATUS[b.status].variant}>{STATUS[b.status].label}</Badge>
                  </TableCell>
                  <TableCell className="min-w-64 whitespace-normal text-muted-foreground">{b.why}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </section>

      <section className="space-y-3">
        <H2>Run it yourself</H2>
        <pre className="overflow-x-auto rounded-lg border bg-muted/40 p-4 font-mono text-xs">
          {`uv sync && cargo build --release -p timelake-cli
uv run timebench list                          # benchmarks, sizes, systems
uv run timebench run tsbs-iot --scale small    # every lake system on your disk, a few minutes
uv run timebench report latest`}
        </pre>
        <p className="text-muted-foreground">
          Setup, storage options, and how to add a benchmark or a system:{" "}
          <a className="underline underline-offset-4" href={`${REPO_URL}/tree/main/bench`}>
            bench/README.md
          </a>
          .
        </p>
      </section>
    </div>
  )
}
