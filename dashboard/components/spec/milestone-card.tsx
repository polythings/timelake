import Link from "next/link"

import { StatusBadge, StatusDot } from "@/components/spec/status-badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { type Milestone, specByNumber, STATUS_LABEL, versionsText } from "@/lib/specs"
import { cn } from "@/lib/utils"

/** A milestone on the roadmap: its status, releases, one line on what it brings, and its specs. */
export function MilestoneCard({ milestone: m }: { milestone: Milestone }) {
  const specs = m.specs.map(specByNumber).filter((s) => s !== undefined)
  const done = specs.filter((s) => s.status === "done").length
  return (
    <Card
      className={cn(
        "relative",
        m.status === "exploring" && "bg-transparent ring-0 outline-1 -outline-offset-1 outline-border outline-dashed",
      )}
    >
      <CardHeader>
        <div className="flex items-start justify-between gap-3">
          <CardTitle>
            <Link href={`/roadmap/${m.slug}`} className="hover:underline hover:underline-offset-4">
              {m.title}
            </Link>
          </CardTitle>
          <StatusBadge status={m.status} />
        </div>
        <CardDescription>{m.description}</CardDescription>
      </CardHeader>
      <CardContent className="mt-auto space-y-2">
        <p className="text-xs text-muted-foreground tabular-nums">
          {m.versions && <>{versionsText(m.versions)} · </>}
          {done} of {specs.length} specs done
        </p>
        <ul className="flex flex-wrap gap-x-3 gap-y-1 text-sm">
          {specs.map((s) => (
            <li key={s.slug}>
              <Link
                href={`/spec/${s.slug}`}
                className="flex items-center gap-1.5 text-muted-foreground hover:text-foreground"
                title={STATUS_LABEL[s.status]}
              >
                <StatusDot status={s.status} />
                {s.title}
              </Link>
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  )
}
