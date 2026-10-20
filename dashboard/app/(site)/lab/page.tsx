import { readFile } from "node:fs/promises"
import path from "node:path"

import { ArrowRight } from "lucide-react"
import type { Metadata } from "next"
import { cacheLife } from "next/cache"
import Link from "next/link"
import { notFound } from "next/navigation"
import { Suspense } from "react"

import { dotPaper } from "@/components/dot-paper"
import { GithubIcon } from "@/components/github-icon"
import { AgentTerminal } from "@/components/lab/agent-terminal"
import { BucketObjects, EngineLegend, PerQuery, QueryMatrix } from "@/components/lab/bench-dots"
import { BucketAnimation } from "@/components/lab/bucket-animation"
import { DotStripChart } from "@/components/lab/dot-strip-chart"
import { DotStats, DotRule, type LoggedRun, RoadmapDots, RunLog, type Stat, Waffle } from "@/components/lab/kit"
import { LiveStripChart } from "@/components/lab/live-strip-chart"
import { SeriesField } from "@/components/lab/series-field"
import { StripChart } from "@/components/lab/strip-chart"
import { type Snapshot, TimeTravel } from "@/components/lab/time-travel"
import { Race, type Racer } from "@/components/race"
import { Button } from "@/components/ui/button"
import {
  SYSTEMS,
  SYSTEM_COLORS,
  engineLabel,
  latestByTarget,
  loadRuns,
  rankings,
  targetLabel,
  tuningOf,
} from "@/lib/benchmarks"
import { REPO_URL } from "@/lib/site"

/** Visual ideas for the home page, side by side: not linked, not indexed, and 404 outside local development. */
export const metadata: Metadata = { title: "Lab", robots: { index: false } }

const BTN = "font-mono text-xs uppercase tracking-[0.06em]"

/** The demo table that the live chart and the time-travel slider read (written with the CLI). */
const DEMO = path.resolve(/*turbopackIgnore: true*/ process.cwd(), "lab-lake/demo/readings/metadata")

/** The hero's heading and buttons, on a panel over a moving background. */
function HeroPanel() {
  return (
    <div className="absolute inset-y-0 left-0 flex items-center p-6 sm:p-8">
      <div className="flex flex-col gap-5 rounded-md border bg-card/90 p-6 backdrop-blur-sm">
        <h1 className="text-4xl leading-[1.05] font-light tracking-tight sm:text-5xl">
          The open time&#8209;series
          <br />
          lakehouse
        </h1>
        <div className="flex flex-wrap gap-2.5">
          <Button size="lg" className={BTN} nativeButton={false} render={<Link href="/docs/quick-start" />}>
            Quick start <ArrowRight />
          </Button>
          <Button
            size="lg"
            variant="outline"
            className={BTN}
            nativeButton={false}
            render={<a href={REPO_URL} target="_blank" rel="noreferrer" />}
          >
            <GithubIcon /> GitHub
          </Button>
        </div>
      </div>
    </div>
  )
}

/** A small readout in the bottom-right corner of a hero background. */
function Chip({ children }: { children: React.ReactNode }) {
  return (
    <div className="absolute right-3 bottom-2 rounded-sm border bg-background/90 px-2 py-1 font-mono text-[10.5px] tracking-[0.08em] text-muted-foreground uppercase">
      {children}
    </div>
  )
}

function Idea({
  n,
  title,
  children,
  note,
}: {
  n: number | string
  title: string
  note: string
  children: React.ReactNode
}) {
  return (
    <section className="flex flex-col gap-4">
      <div className="flex flex-col gap-1.5">
        <div className="label-mono">Idea {n}</div>
        <h2 className="text-2xl leading-tight font-light tracking-tight">{title}</h2>
        <p className="max-w-3xl text-sm text-muted-foreground">{note}</p>
      </div>
      {children}
    </section>
  )
}

export default function Lab() {
  // Ideas kept for later, visible only in local development.
  if (process.env.NODE_ENV === "production") notFound()
  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-20 px-4 pt-10 pb-24 sm:pt-16">
      <div className="flex flex-col gap-2">
        <h1 className="text-3xl font-light tracking-tight">Lab</h1>
        <p className="max-w-3xl text-muted-foreground">
          Visual ideas for the home page, to compare side by side. The first four are hero backgrounds; the last three
          are for the sections below it.
        </p>
      </div>

      <Idea
        n={1}
        title="Strip-chart recorder"
        note="Chart paper with a pen trace that the paper carries to the left, like the instruments that recorded time series before screens. Generated data. The readout in the corner carries the licence line."
      >
        <div className="cells">
          <StripChart>
            <HeroPanel />
            <Chip>Open source · Apache 2.0 · Iceberg compatible</Chip>
          </StripChart>
        </div>
      </Idea>

      <Idea
        n="1b"
        title="Strip-chart recorder on a dot grid"
        note="The same recorder on the races' grid of squares: the trace lights the squares it passes through, and the area under it glows faintly. Generated data."
      >
        <div className="cells">
          <DotStripChart>
            <HeroPanel />
            <Chip>Open source · Apache 2.0 · Iceberg compatible</Chip>
          </DotStripChart>
        </div>
      </Idea>

      <Idea
        n="1c"
        title="Dot grid, darker and slow"
        note="Idea 1b with the background squares close to black and the trace moving one column a second, so it reads as still until you look for it."
      >
        <div className="cells">
          <DotStripChart tick={1000} paperAlpha={0.35}>
            <HeroPanel />
            <Chip>Open source · Apache 2.0 · Iceberg compatible</Chip>
          </DotStripChart>
        </div>
      </Idea>

      <Idea
        n={2}
        title="The recorder, drawing real data read in your browser"
        note="The same recorder, but the trace is a week of a sensor's 10-minute averages read from a real Timelake table in this browser with @timelake/client. The readout in the corner says what the read took. The table is in dashboard/lab-lake (written with the CLI); on the live site it would sit in a public bucket. For later, once a table is streaming new data."
      >
        <div className="cells">
          <LiveStripChart>
            <HeroPanel />
          </LiveStripChart>
        </div>
      </Idea>

      <Idea
        n={3}
        title="A field of series"
        note="The mark's idea as a background: parallel lines that turn together at sharp corners, like readings from one machine, drifting slowly. One line is in the accent."
      >
        <div className="cells">
          <SeriesField>
            <HeroPanel />
          </SeriesField>
        </div>
      </Idea>

      <Idea
        n={4}
        title="Agents query it with the CLI"
        note="An agent answers a question by running timelake commands; the commands ran on the demo table and the output is theirs. Next to it, two thousand agents doing the same, with no database between them and the data."
      >
        <AgentTerminal />
      </Idea>

      <Idea
        n={5}
        title="A bucket that maintains itself"
        note="For the docs (How it works): appends add small files, the writer merges them, retention drops the oldest day, and each commit writes new metadata and a new head. Illustrative names and sizes."
      >
        <BucketAnimation />
      </Idea>

      <Suspense fallback={null}>
        <Races />
      </Suspense>

      <Suspense fallback={null}>
        <Travel />
      </Suspense>

      <Suspense fallback={null}>
        <StaticCharts />
      </Suspense>

      <Suspense fallback={null}>
        <Kit />
      </Suspense>
    </div>
  )
}

/** The newest weekly run of TSBS IoT on AWS S3. */
async function latestS3() {
  return latestByTarget(await loadRuns(), "tsbs-iot").find(
    (r) => r.header.target === "aws-s3" && !r.header.variant && tuningOf(r) === "tuned",
  )
}

/** The same run as still pictures: objects in the bucket, every query, and requests and bytes per query. */
async function StaticCharts() {
  const run = await latestS3()
  if (!run) return null
  return (
    <>
      <Idea
        n={8}
        title="Every object in the bucket"
        note="Static, in each system's color: one square per object after streaming, data files in full color and metadata dimmed. Real numbers from the weekly run."
      >
        <BucketObjects run={run} />
      </Idea>

      <Idea
        n={9}
        title="Every query, every engine"
        note="All 17 queries against every engine as one picture: a square per pair in the engine's color, as bright as it is fast (the fastest on each query is full strength). Hover a square for its time. A system read through another engine is drawn at half strength."
      >
        <div className="flex flex-col gap-6 rounded-md border bg-card p-6">
          <EngineLegend run={run} />
          <div className="grid gap-10 lg:grid-cols-2">
            <QueryMatrix run={run} checkpoint="ingested" title="Right after ingest" />
            <QueryMatrix run={run} checkpoint="maintained" title="After maintenance" />
          </div>
        </div>
      </Idea>

      <Idea
        n={10}
        title="Requests and data read, per query"
        note="Every query on each system's own reader, right after ingest: one lane per system, a square per 25 requests and per 3 MB read. Hover a lane for its number."
      >
        <div className="flex flex-col gap-4">
          <EngineLegend run={run} />
          <PerQuery run={run} checkpoint="ingested" />
        </div>
      </Idea>
    </>
  )
}

/** Where each benchmark target's runs show in the run log. */
const TARGET_COLORS: Record<string, string> = {
  "aws-s3": "var(--primary)",
  "aws-s3x": "var(--chart-5)",
  local: "var(--chart-2)",
  s3: "var(--chart-3)",
  "tiger-cloud": "var(--chart-4)",
}

/** Smaller elements in the same dot style, for anywhere on the site. */
async function Kit() {
  const runs = await loadRuns()
  const run = await latestS3()
  const ranked = run ? rankings(run) : []
  const own = (prefix: string) => ranked.find((r) => r.title.startsWith(prefix))?.rows.find((r) => r.timelake)
  const fresh = ranked.find((r) => r.title.startsWith("Queries, right"))?.rows ?? []
  const freshOwn = fresh.find((r) => r.timelake)?.value
  const others = fresh.filter((r) => r.system !== "timelake").map((r) => r.value)
  const lastLoc = (engine: string) =>
    run?.queries.find(
      (q) => `${q.system}+${q.reader}` === engine && q.query === "last-loc" && q.checkpoint === "ingested",
    )
  const mine = lastLoc("timelake+timelake")
  const iceberg = lastLoc("iceberg+duckdb")
  const maintained = own("Queries, after")?.value
  const commit = own("Commit")?.value
  const stats: Stat[] = [
    maintained && {
      value: `${Math.round(maintained * 1000)} ms`,
      label: "Per query, after maintenance",
      note: "Geometric mean of 17 TSBS IoT queries on S3.",
    },
    freshOwn &&
      others.length > 0 && {
        value: `${Math.round(Math.min(...others) / freshOwn)}–${Math.round(Math.max(...others) / freshOwn)}×`,
        label: "Faster on fresh data",
        note: "Than Iceberg, Lance, and Parquet files, right after ingest.",
      },
    mine?.requests && {
      value: `${mine.requests[0]} req`,
      label: `Latest position of ${mine.rows?.toLocaleString("en")} trucks`,
      note: `Iceberg + DuckDB: ${iceberg?.requests?.[0]?.toLocaleString("en")} requests.`,
    },
    commit && { value: `${Math.round(commit)} ms`, label: "Median commit", note: "While streaming to S3." },
  ].filter((s): s is Stat => !!s)
  const tlBytes = mine?.bytes?.[0]
  const iceBytes = iceberg?.bytes?.[0]
  const logged: LoggedRun[] = runs
    .map((r) => ({
      id: r.header.id,
      date: r.header.started.slice(0, 10),
      target: targetLabel(r.header.target),
      workload: r.header.workload,
      color: TARGET_COLORS[r.header.target] ?? "var(--chart-2)",
    }))
    .toReversed()
  return (
    <>
      <div className="flex flex-col gap-2 border-t pt-12">
        <h2 className="text-3xl font-light tracking-tight">Style kit</h2>
        <p className="max-w-3xl text-muted-foreground">
          Smaller pieces in the same dot style, for the home page and the rest of the site. All static.
        </p>
      </div>

      <Idea
        n="K1"
        title="Dot paper"
        note="A still background of dim squares, the races' grid without the motion. Here behind the hero, with the licence line as a readout."
      >
        <div className="cells">
          <div className="relative h-[234px]" style={dotPaper()}>
            <HeroPanel />
            <Chip>Open source · Apache 2.0 · Iceberg compatible</Chip>
          </div>
        </div>
      </Idea>

      <Idea
        n="K2"
        title="Dot-matrix numbers"
        note="Key numbers in a 5×7 dot font, from the newest weekly run, for stat cards or the benefit cards."
      >
        <DotStats stats={stats} />
      </Idea>

      {tlBytes && iceBytes ? (
        <Idea
          n="K3"
          title="Waffle"
          note="One amount as a block of squares and a smaller one lit inside it. Data read for the latest position of every truck, right after ingest."
        >
          <Waffle
            lit={Math.max(1, Math.round((tlBytes / iceBytes) * 1000))}
            total={1000}
            perRow={50}
            litColor="var(--primary)"
            restColor={SYSTEM_COLORS.iceberg ?? "var(--chart-3)"}
          >
            <p>
              The block is the {(iceBytes / 1e6).toFixed(0)} MB that Iceberg + DuckDB read; each square is a thousandth
              of it.
            </p>
            <p>
              The lit square is Timelake: <span className="text-foreground">{(tlBytes / 1e6).toFixed(2)} MB</span>, read
              from its last-value index.
            </p>
          </Waffle>
        </Idea>
      ) : null}

      <Idea
        n="K4"
        title="Roadmap in squares"
        note="Each milestone as a square, filled as far as it has come: done, in progress, planned, exploring. Hover for the description."
      >
        <RoadmapDots />
      </Idea>

      <Idea
        n="K5"
        title="Run log"
        note="Every benchmark run as a square, colored by where it ran, newest first. Grows every week."
      >
        <RunLog runs={logged} />
      </Idea>

      <Idea n="K6" title="Divider" note="A thin band of dot paper that fades at both ends, between sections.">
        <DotRule />
      </Idea>
    </>
  )
}

/** Each system's own reader, then the others reading through DuckDB. */
const ENGINES = ["timelake+timelake", "lance+duckdb", "iceberg+duckdb", "parquet+duckdb"]
const SYSTEM_ORDER = ["timelake", "lance", "iceberg", "parquet"]
/** A system's chart color, from "system" or "system+reader". */
const colorOf = (engine: string) => SYSTEM_COLORS[engine.split("+")[0] ?? ""] ?? "var(--chart-2)"

/** Seconds of streaming that the commit race counts commits in. */
const COMMIT_WINDOW = 10

/** Races from the newest weekly S3 run: one query's requests and bytes, then files and commits while streaming. */
async function Races() {
  const run = await latestS3()
  if (!run) return null
  const lastLoc = ENGINES.flatMap((e) => {
    const q = run.queries.find(
      (x) =>
        `${x.system}+${x.reader}` === e && x.query === "last-loc" && x.checkpoint === "ingested" && x.status === "ok",
    )
    return q ? [{ e, q }] : []
  })
  const requests: Racer[] = lastLoc.flatMap(({ e, q }) => {
    const n = q.requests?.[0]
    return n === undefined
      ? []
      : [
          {
            label: engineLabel(e),
            value: n,
            dots: n,
            seconds: q.times[0],
            timelake: e === "timelake+timelake",
            color: colorOf(e),
          },
        ]
  })
  // A dot is 100 KB read.
  const bytes: Racer[] = lastLoc.flatMap(({ e, q }) => {
    const b = q.bytes?.[0]
    return b === undefined
      ? []
      : [
          {
            label: engineLabel(e),
            value: b / 1e6,
            dots: Math.max(1, Math.round(b / 1e5)),
            seconds: q.times[0],
            timelake: e === "timelake+timelake",
            color: colorOf(e),
          },
        ]
  })
  const ingest = SYSTEM_ORDER.flatMap((s) => {
    const p = run.phases.find((x) => x.kind === "ingest" && x.system === s && x.status === "ok")
    return p ? [{ s, p }] : []
  })
  const files: Racer[] = ingest.flatMap(({ s, p }) =>
    p.data_files === undefined || p.seconds === undefined
      ? []
      : [
          {
            label: SYSTEMS[s] ?? s,
            value: p.data_files,
            dots: p.data_files,
            seconds: p.seconds,
            timelake: s === "timelake",
            color: colorOf(s),
          },
        ],
  )
  const commits: Racer[] = ingest.flatMap(({ s, p }) => {
    if (!p.commit_ms_p50) return []
    const n = Math.floor((COMMIT_WINDOW * 1000) / p.commit_ms_p50)
    return [
      {
        label: SYSTEMS[s] ?? s,
        value: n,
        dots: n,
        seconds: COMMIT_WINDOW,
        timelake: s === "timelake",
        color: colorOf(s),
      },
    ]
  })
  const trucks = lastLoc[0]?.q.rows?.toLocaleString("en") ?? "every"
  const date = run.header.started.slice(0, 10)
  const timelakeIngest = ingest.find((x) => x.s === "timelake")?.p
  return (
    <Idea
      n={6}
      title="Races"
      note="For “How fast is it?”: the weekly run's numbers as races, each unit a dot, with the time each system took. All real numbers from the run below."
    >
      <div className="flex flex-col gap-6">
        <Race
          racers={requests}
          unit="requests"
          caption={`Requests to answer “latest position of ${trucks} trucks”, right after ingest. TSBS IoT on AWS S3, run of ${date}; the first run's times.`}
        />
        <Race
          racers={bytes}
          unit="MB"
          decimals={1}
          caption={`Data read from S3 for the same query. Each dot is 100 KB. Timelake reads its last-value index; the others read every data file.`}
        />
        <Race
          racers={files}
          unit="data files"
          caption={`Data files in the bucket after streaming the same ${timelakeIngest?.commits?.toLocaleString("en") ?? ""} commits, before any maintenance job. Timelake merges small files as it writes; the others leave one file per commit. Times are how long each took to take in the stream.`}
        />
        <Race
          racers={commits}
          unit="commits"
          caption={`Commits in ${COMMIT_WINDOW} seconds of streaming, at each system's median commit time in the run.`}
        />
      </div>
    </Idea>
  )
}

/** The demo table's snapshots, from its current metadata file. */
async function loadSnapshots(): Promise<{ snapshots: Snapshot[]; days: string[] }> {
  "use cache"
  cacheLife("max")
  const version = (await readFile(path.join(DEMO, "version-hint.text"), "utf8")).trim()
  // Snapshot ids don't fit in a JavaScript number; keep them as strings.
  const text = (await readFile(path.join(DEMO, `v${version}.metadata.json`), "utf8")).replace(
    /"snapshot-id"\s*:\s*(\d+)/g,
    '"snapshot-id":"$1"',
  )
  const meta = JSON.parse(text) as {
    snapshots: { "snapshot-id": string; "sequence-number": number; summary: Record<string, string> }[]
  }
  const snapshots = meta.snapshots
    .map((s) => ({
      // Version 1 is the empty table; each commit after it adds one.
      version: s["sequence-number"] + 1,
      id: s["snapshot-id"],
      operation: s.summary.operation ?? "",
      added: Number(s.summary["added-records"] ?? 0),
      rows: Number(s.summary["total-records"] ?? 0),
      files: Number(s.summary["total-data-files"] ?? 0),
    }))
    .sort((a, b) => a.version - b.version)
  const days = Array.from({ length: Math.max(...snapshots.map((s) => s.files)) }, (_, i) =>
    new Date(Date.UTC(2026, 9, 9 + i)).toISOString().slice(0, 10),
  )
  return { snapshots, days }
}

async function Travel() {
  const { snapshots, days } = await loadSnapshots()
  return (
    <Idea
      n={7}
      title="Time travel"
      note="Every commit is a version of the table. Drag through the demo table's seven commits (a day each); the numbers come from its Iceberg metadata, and any version can be made current again with timelake rollback."
    >
      <TimeTravel snapshots={snapshots} days={days} />
    </Idea>
  )
}
