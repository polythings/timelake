import type { Metadata } from "next"
import Link from "next/link"

import { GithubIcon } from "@/components/github-icon"
import { MilestoneCard } from "@/components/spec/milestone-card"
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

export default function RoadmapPage() {
  return (
    <div className="mx-auto max-w-6xl space-y-14 px-4 py-12">
      <header className="max-w-3xl space-y-3">
        <h1 className="text-3xl font-semibold tracking-tight">Roadmap</h1>
        <p className="text-muted-foreground">
          What we&apos;re building, as milestones made of{" "}
          <Link className="underline underline-offset-4" href="/spec">
            specs
          </Link>
          , one per feature. Each milestone links its specs, and each spec the releases that changed it. Updated{" "}
          {UPDATED}.
        </p>
      </header>

      {(Object.keys(MILESTONE_STATUS_LABEL) as MilestoneStatus[]).map((status) => {
        // Done milestones newest first; the others in order of priority.
        const listed = MILESTONES.filter((m) => m.status === status).toSorted((x, y) =>
          status === "done" ? y.number - x.number : x.number - y.number,
        )
        return (
          listed.length > 0 && (
            <section key={status} className="space-y-4">
              <div className="space-y-1">
                <h2 className="text-xl font-medium tracking-tight">{MILESTONE_STATUS_LABEL[status]}</h2>
                <p className="text-sm text-muted-foreground">{MILESTONE_STATUS_MEANING[status]}</p>
              </div>
              <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
                {listed.map((m) => (
                  <MilestoneCard key={m.slug} milestone={m} />
                ))}
              </div>
            </section>
          )
        )
      })}

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
