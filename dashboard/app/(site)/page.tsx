import {
  ArrowRight,
  Bot,
  BotMessageSquare,
  BrainCircuit,
  ChartCandlestick,
  Factory,
  LayoutDashboard,
  type LucideIcon,
  Server,
  Truck,
  Zap,
} from "lucide-react"
import type { Metadata } from "next"
import Link from "next/link"
import { Suspense } from "react"

import { BenchCards } from "@/components/bench-cards"
import { BenefitCards } from "@/components/benefit-cards"
import { GithubIcon } from "@/components/github-icon"
import { HeroFlow } from "@/components/hero-flow"
import { MoreLink, SectionHead } from "@/components/section-head"
import { StackDiagram } from "@/components/stack-diagram"
import { Button } from "@/components/ui/button"
import { type ArtKind, UseCaseArt } from "@/components/use-case-art"
import { latestByTarget, loadRuns } from "@/lib/benchmarks"
import { REPO_URL, SITE_NAME, SITE_URL } from "@/lib/site"

export const metadata: Metadata = { alternates: { canonical: "/" } }

// Names the site in search results (otherwise they show the domain).
const WEBSITE = { "@context": "https://schema.org", "@type": "WebSite", name: SITE_NAME, url: `${SITE_URL}/` }

const BTN = "font-mono text-xs uppercase tracking-[0.06em]"

type UseCase = { icon: LucideIcon; art: ArtKind; title: string; body: string }

/** The data that comes in as time series, each with a picture drawn in squares. */
const DATA: UseCase[] = [
  {
    icon: Factory,
    art: "sensors",
    title: "Industrial IoT",
    body: "Thousands of sensor tags sampled every second to every minute, with backlogs sent after outages.",
  },
  {
    icon: Truck,
    art: "fleet",
    title: "Vehicle fleets",
    body: "Position and diagnostics from thousands of vehicles and devices, every few seconds.",
  },
  {
    icon: Bot,
    art: "robot",
    title: "Robotics",
    body: "Joint positions, torques, and sensor readings from robots, recorded at high rates and replayed by time range.",
  },
  {
    icon: ChartCandlestick,
    art: "market",
    title: "Financial data",
    body: "Prices and trades for thousands of instruments, read as time ranges, bars, and the latest price.",
  },
  {
    icon: Server,
    art: "metrics",
    title: "Server metrics",
    body: "CPU, memory, and disk metrics from thousands of hosts.",
  },
]

/** The ways that data gets read. */
const USES: UseCase[] = [
  {
    icon: LayoutDashboard,
    art: "dashboard",
    title: "Dashboards and apps",
    body: "Charts and apps that read the bucket over HTTPS with the TypeScript client, in the browser or at the edge.",
  },
  {
    icon: BrainCircuit,
    art: "ml",
    title: "Data science and ML",
    body: "Time series pulled into notebooks as Arrow tables, for pandas, polars, and model training, straight from the bucket.",
  },
  {
    icon: Zap,
    art: "functions",
    title: "Serverless functions",
    body: "Lambda functions and short jobs that start, append or query, and exit. There are no connections to pool and no database to size.",
  },
  {
    icon: BotMessageSquare,
    art: "agents",
    title: "AI agents",
    body: "Agents can query as often as they like: reads go to object storage, so there is no database for them to overload.",
  },
]

/** A row of use-case cards, each with its picture. */
function UseCaseGrid({ cases, columns }: { cases: UseCase[]; columns: string }) {
  return (
    <div className={`grid gap-4 sm:grid-cols-2 ${columns}`}>
      {cases.map((u) => {
        const Icon = u.icon
        return (
          <div key={u.title} data-use-case className="flex flex-col gap-2 rounded-lg border bg-card p-6">
            <div className="mb-3">
              <UseCaseArt kind={u.art} />
            </div>
            <h3 className="flex items-center gap-2.5 text-lg font-normal">
              <Icon className="size-4.5 shrink-0 text-muted-foreground" aria-hidden />
              {u.title}
            </h3>
            <p className="text-sm leading-relaxed text-muted-foreground">{u.body}</p>
          </div>
        )
      })}
    </div>
  )
}

export default function Home() {
  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-16 px-4 pt-8 pb-24">
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(WEBSITE) }} />
      <section className="flex flex-col gap-8">
        <div className="flex flex-col gap-5 sm:flex-row sm:items-center sm:justify-between">
          <h1 className="text-4xl leading-[1.05] font-light tracking-tight sm:text-5xl">
            The open time&#8209;series
            <br />
            lakehouse
          </h1>
          <div className="flex shrink-0 flex-wrap gap-2.5">
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
        {/* Publish on the left by hovering the chart; the store in the middle; readers as tabs on the right. */}
        <div className="cells">
          <div className="relative h-[160px] overflow-hidden sm:h-[176px]">
            <HeroFlow />
          </div>
        </div>
      </section>

      <section className="flex flex-col gap-4">
        <SectionHead label="Features">
          Fast, cheap time-series queries on open files in your own bucket, with no server to run.
        </SectionHead>
        <BenefitCards />
      </section>

      <section className="flex flex-col gap-4">
        <SectionHead label="Data">Time series from machines, vehicles, markets, and servers.</SectionHead>
        <UseCaseGrid cases={DATA} columns="lg:grid-cols-5" />
      </section>

      <section className="flex flex-col gap-4">
        <SectionHead label="Uses">
          Read straight from your bucket by apps, notebooks, functions, and agents.
        </SectionHead>
        <UseCaseGrid cases={USES} columns="lg:grid-cols-4" />
      </section>

      <section className="flex flex-col gap-4">
        <SectionHead label="Architecture">
          From the sources that send data to the tools that read it, with only your bucket in between.
        </SectionHead>
        <StackDiagram />
      </section>

      <Suspense fallback={null}>
        <Measurements />
      </Suspense>
    </div>
  )
}

/** The latest weekly run of TSBS IoT on S3: what was measured, and the results. */
async function Measurements() {
  const run = latestByTarget(await loadRuns(), "tsbs-iot").find(
    (r) => r.header.target === "aws-s3" && !r.header.variant,
  )
  if (!run) return null
  return (
    <section className="flex flex-col gap-4">
      <div className="flex flex-col gap-3">
        <SectionHead label="Benchmarks">
          Measured every week on AWS S3, against other lakes with the same data and queries.
        </SectionHead>
        <nav className="flex flex-wrap gap-x-5 gap-y-1">
          <MoreLink href="/benchmarks">All benchmarks</MoreLink>
          <MoreLink href="/benchmarks/timebench">How we measure</MoreLink>
        </nav>
      </div>
      <BenchCards run={run} />
    </section>
  )
}
