import type { Metadata } from "next"
import Link from "next/link"

import { GithubIcon } from "@/components/github-icon"
import { PageTitle } from "@/components/page-title"
import { MilestoneCard } from "@/components/spec/milestone-card"
import { StatusLegend } from "@/components/spec/status-badge"
import { Button } from "@/components/ui/button"
import { REPO_URL } from "@/lib/site"
import { MILESTONE_STATUS_LABEL, MILESTONE_STATUS_MEANING, MILESTONES, type MilestoneStatus } from "@/lib/specs"

export const metadata: Metadata = {
  title: "Roadmap",
  description: "Timelake's milestones: what is being built now, what comes next, and what is done, each made of specs.",
}

const UPDATED = MILESTONES.map((m) => m.updated)
  .toSorted()
  .at(-1)

/** The board's columns, in the order a milestone moves through them. */
const COLUMNS: MilestoneStatus[] = ["exploring", "planned", "in-progress", "done"]

export default function RoadmapPage() {
  return (
    <div className="mx-auto max-w-6xl space-y-14 px-4 py-12">
      <header className="max-w-3xl space-y-3">
        <PageTitle>Roadmap</PageTitle>
        <p className="text-muted-foreground">
          What we&apos;re building, as milestones made of{" "}
          <Link className="underline underline-offset-4" href="/spec">
            specs
          </Link>
          , one per feature. Each square on a milestone is one of its specs. Updated {UPDATED}.
        </p>
        <StatusLegend />
      </header>

      <div className="grid items-start gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {COLUMNS.map((status) => {
          // Done milestones newest first; the others in order of priority.
          const listed = MILESTONES.filter((m) => m.status === status).toSorted((x, y) =>
            status === "done" ? y.number - x.number : x.number - y.number,
          )
          return (
            <section key={status} className="flex flex-col gap-3 rounded-lg bg-muted/50 p-3">
              <div className="space-y-0.5 px-0.5">
                <h2 className="flex items-baseline justify-between gap-2 text-sm font-medium">
                  {MILESTONE_STATUS_LABEL[status]}
                  <span className="font-mono text-xs font-normal text-muted-foreground tabular-nums">
                    {listed.length}
                  </span>
                </h2>
                <p className="text-xs text-muted-foreground">{MILESTONE_STATUS_MEANING[status]}</p>
              </div>
              <div className="flex flex-col gap-2">
                {listed.map((m) => (
                  <MilestoneCard key={m.slug} milestone={m} />
                ))}
              </div>
            </section>
          )
        })}
      </div>

      <section className="max-w-3xl space-y-4 border-t pt-10">
        <h2 className="text-xl font-medium tracking-tight">Build it with us</h2>
        <p className="text-muted-foreground">
          Missing something you need, or want to help? Open an issue, read the design behind each item, or see how to
          contribute.
        </p>
        <div className="flex flex-wrap gap-3">
          <Button nativeButton={false} render={<a href={`${REPO_URL}/issues/new`} target="_blank" rel="noreferrer" />}>
            <GithubIcon />
            Open an issue
          </Button>
          <Button variant="outline" nativeButton={false} render={<Link href="/spec" />}>
            Read the design
          </Button>
          <Button variant="outline" nativeButton={false} render={<Link href="/contribute" />}>
            How to contribute
          </Button>
          <Button variant="outline" nativeButton={false} render={<Link href="/docs/changelog" />}>
            Changelog
          </Button>
        </div>
      </section>
    </div>
  )
}
