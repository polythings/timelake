import { Activity, ArrowRight, Cloud, Gauge, Layers, PiggyBank, ServerOff, type LucideIcon } from "lucide-react"
import Link from "next/link"

import { type ArtKind, UseCaseArt } from "@/components/use-case-art"
import { latestByTarget, loadRuns, rankings, tuningOf } from "@/lib/benchmarks"

type Card = {
  icon: LucideIcon
  art: ArtKind
  title: string
  body: string
  link: { label: string; href: string }
}

/** Timelake's query times in the newest weekly S3 run, against the other systems in it. */
async function speed(): Promise<string> {
  const run = latestByTarget(await loadRuns(), "tsbs-iot").find(
    (r) => r.header.target === "aws-s3" && !r.header.variant && tuningOf(r) === "tuned",
  )
  const ranked = run ? rankings(run) : []
  const rows = (prefix: string) => ranked.find((r) => r.title.startsWith(prefix))?.rows ?? []
  const maintained = rows("Queries, after").find((r) => r.timelake)
  const ingested = rows("Queries, right")
  const own = ingested.find((r) => r.timelake)
  // Each other system's best reader, as the benchmark cards show them.
  const others = ingested
    .filter((r, i, all) => r.system !== "timelake" && all.findIndex((x) => x.system === r.system) === i)
    .map((r) => r.value)
  if (!maintained || !own || others.length === 0) return "Measured every week against other lakes on S3."
  const x = (v: number) => Math.round(v / own.value)
  const range = `${x(Math.min(...others))}⁠–⁠${x(Math.max(...others))}×`
  return `Queries on S3 return in ${Math.round(maintained.value * 1000)} ms, and are ${range} faster than other lakes on fresh data.`
}

/** Columns in a card's picture: as many 4 px squares as fit a third of the page. */
const CARD_COLS = 76

export async function BenefitCards() {
  // Two rows of three.
  const cards: Card[] = [
    {
      icon: Gauge,
      art: "fast",
      title: "Fast",
      body: await speed(),
      link: { label: "Benchmarks", href: "/benchmarks" },
    },
    {
      icon: PiggyBank,
      art: "cheap",
      title: "Cheap",
      body: "You pay only for storage, reads, and writes. Data costs about 6× less to keep than in TimescaleDB.",
      link: { label: "Cost", href: "/docs/storage#requests-and-cost" },
    },
    {
      icon: Activity,
      art: "layout",
      title: "Analytics",
      body: "An optimized toolkit for analyzing time series, built into the engine and storage.",
      link: { label: "Performance", href: "/docs/performance" },
    },
    {
      icon: Layers,
      art: "formats",
      title: "Open",
      body: "Standard Iceberg and Parquet in your own bucket, so you’re not locked in.",
      link: { label: "Querying", href: "/docs/querying" },
    },
    {
      icon: ServerOff,
      art: "serverless",
      title: "Serverless",
      body: "No database or maintenance jobs to run. Timelake is a library that runs in your process.",
      link: { label: "How it works", href: "/docs/how-it-works" },
    },
    {
      icon: Cloud,
      art: "stores",
      title: "Any object store",
      body: "Runs on AWS S3, Google Cloud Storage, Azure, R2, MinIO, or your local disk.",
      link: { label: "Storage", href: "/docs/storage" },
    },
  ]
  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
      {cards.map((c) => {
        const Icon = c.icon
        return (
          <div key={c.title} data-use-case className="flex flex-col gap-2 rounded-lg border bg-card p-6">
            <div className="mb-3">
              <UseCaseArt kind={c.art} cols={CARD_COLS} />
            </div>
            <h3 className="flex items-center gap-2.5 text-lg font-normal">
              <Icon className="size-4.5 text-muted-foreground" aria-hidden />
              {c.title}
            </h3>
            <p className="text-sm leading-relaxed text-muted-foreground">{c.body}</p>
            <Link
              href={c.link.href}
              {...(c.link.href.startsWith("http") ? { target: "_blank", rel: "noreferrer" } : {})}
              className="mt-auto flex w-fit items-center gap-1.5 pt-2 text-sm hover:underline"
            >
              {c.link.label} <ArrowRight className="size-3.5" />
            </Link>
          </div>
        )
      })}
    </div>
  )
}
