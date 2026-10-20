import { Check, CircleDashed, CircleDot, Telescope, X } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { MILESTONE_STATUS_LABEL, type MilestoneStatus, STATUS_LABEL, type SpecStatus } from "@/lib/specs"

const ICON = { done: Check, "in-progress": CircleDot, planned: CircleDashed, dropped: X, exploring: Telescope }

/** A spec's or a milestone's status. */
export function StatusBadge({ status }: { status: SpecStatus | MilestoneStatus }) {
  const Icon = ICON[status]
  const label = status === "exploring" ? MILESTONE_STATUS_LABEL[status] : STATUS_LABEL[status]
  return (
    <Badge variant={status === "done" ? "secondary" : "outline"} className={status === "dropped" ? "opacity-70" : ""}>
      <Icon data-icon="inline-start" />
      {label}
    </Badge>
  )
}

const DOT: Record<SpecStatus, string> = {
  done: "bg-foreground",
  "in-progress": "bg-foreground/50",
  planned: "border border-foreground/40",
  dropped: "bg-transparent",
}

/** A small square for a spec's status, where a badge would be too much; screen readers hear the status. */
export function StatusDot({ status }: { status: SpecStatus }) {
  return (
    <>
      <span className={`size-2 shrink-0 ${DOT[status]}`} aria-hidden />
      <span className="sr-only">{STATUS_LABEL[status]}:</span>
    </>
  )
}

/** What the status squares mean, wherever a list of them is shown. */
export function StatusLegend({ className }: { className?: string }) {
  const shown: SpecStatus[] = ["done", "in-progress", "planned"]
  return (
    <p className={`flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground ${className ?? ""}`}>
      {shown.map((s) => (
        <span key={s} className="flex items-center gap-1.5">
          <span className={`size-2 shrink-0 ${DOT[s]}`} aria-hidden />
          {STATUS_LABEL[s]}
        </span>
      ))}
    </p>
  )
}
