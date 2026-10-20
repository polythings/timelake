import Link from "next/link"

import { StatusDot } from "@/components/spec/status-badge"
import { type Milestone, specByNumber, versionsText } from "@/lib/specs"
import { cn } from "@/lib/utils"

/** A milestone on the roadmap board: its title, a square per spec for its status, and its releases. */
export function MilestoneCard({ milestone: m }: { milestone: Milestone }) {
  const specs = m.specs.map(specByNumber).filter((s) => s !== undefined)
  const done = specs.filter((s) => s.status === "done").length
  return (
    <Link
      href={`/roadmap/${m.slug}`}
      className={cn(
        "flex flex-col gap-2.5 rounded-lg border bg-card p-3.5 transition-colors hover:border-foreground/30",
        m.status === "exploring" && "border-dashed bg-transparent",
      )}
    >
      <span className="text-sm leading-snug font-medium">{m.title}</span>
      <span className="flex flex-wrap items-center gap-x-3 gap-y-1.5 text-xs text-muted-foreground tabular-nums">
        <span className="flex flex-wrap items-center gap-1" aria-hidden>
          {specs.map((s) => (
            <StatusDot key={s.slug} status={s.status} />
          ))}
        </span>
        <span>
          {done}/{specs.length} specs<span className="sr-only"> done</span>
        </span>
        {m.versions && <span className="ml-auto">{versionsText(m.versions)}</span>}
      </span>
    </Link>
  )
}
