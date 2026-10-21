import { ArrowUpRight } from "lucide-react"

import { StatusBadge } from "@/components/spec/status-badge"
import { REPO_URL } from "@/lib/site"
import type { MilestoneStatus, SpecStatus } from "@/lib/specs"

/** A spec page's navigation, at its left on wide screens, staying in view as the page scrolls. */
export function SpecAside({ children }: { children: React.ReactNode }) {
  return (
    <aside className="sticky top-20 hidden h-[calc(100svh-6rem)] w-60 shrink-0 overflow-y-auto md:block">
      {children}
    </aside>
  )
}

/** Under a spec's or milestone's title: its status, releases, last update, and its file on GitHub (`path` in the repository). */
export function SpecMeta({
  status,
  versions,
  updated,
  path,
}: {
  status: SpecStatus | MilestoneStatus
  versions?: string
  updated: string
  path: string
}) {
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-2 text-sm text-muted-foreground">
      <StatusBadge status={status} />
      {versions && <span className="tabular-nums">{versions}</span>}
      <span className="tabular-nums">Updated {updated}</span>
      <a className="inline-flex items-center gap-0.5 hover:text-foreground" href={`${REPO_URL}/blob/main/${path}`}>
        View on GitHub
        <ArrowUpRight className="size-3.5" />
      </a>
    </div>
  )
}
