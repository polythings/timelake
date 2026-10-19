import { Check, CircleDashed, CircleDot, X } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { STATUS_LABEL, type SpecStatus } from "@/lib/specs"

const ICON = { done: Check, "in-progress": CircleDot, planned: CircleDashed, dropped: X }

export function StatusBadge({ status }: { status: SpecStatus }) {
  const Icon = ICON[status]
  return (
    <Badge variant={status === "done" ? "secondary" : "outline"} className={status === "dropped" ? "opacity-70" : ""}>
      <Icon data-icon="inline-start" />
      {STATUS_LABEL[status]}
    </Badge>
  )
}
