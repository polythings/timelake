import { dotPaper } from "@/components/dot-paper"
import { DotGrid, DotText } from "@/components/dots"
import { type Milestone, MILESTONES } from "@/lib/specs"
import { cn } from "@/lib/utils"

/** A number in the dot-matrix font, with what it measures under it. */
export type Stat = { value: string; label: string; note: string }

export function DotStats({ stats }: { stats: Stat[] }) {
  return (
    <div className="cells sm:grid-cols-2 lg:grid-cols-4">
      {stats.map((s) => (
        <div key={s.label} className="flex flex-col gap-3 p-5">
          <DotText text={s.value} dot={3} />
          <div className="text-sm">{s.label}</div>
          <div className="text-xs leading-relaxed text-muted-foreground">{s.note}</div>
        </div>
      ))}
    </div>
  )
}

/**
 * A waffle of `total` squares standing for one amount, with `lit` of them in the accent standing for a smaller one.
 */
export function Waffle({
  lit,
  total,
  perRow,
  litColor,
  restColor,
  children,
}: {
  lit: number
  total: number
  perRow: number
  litColor: string
  restColor: string
  children: React.ReactNode
}) {
  return (
    <div className="flex flex-col gap-5 rounded-md border bg-card p-6 md:flex-row md:items-center">
      <DotGrid
        dot={6}
        gap={2}
        perRow={perRow}
        segments={[
          { count: lit, color: litColor },
          { count: total - lit, color: restColor, alpha: 0.45 },
        ]}
      />
      <div className="flex flex-col gap-2 text-sm text-muted-foreground">{children}</div>
    </div>
  )
}

const KINDS: { title: string; items: Milestone[]; square: string }[] = [
  { title: "Done", items: MILESTONES.filter((m) => m.status === "done"), square: "bg-primary" },
  {
    title: "In progress",
    items: MILESTONES.filter((m) => m.status === "in-progress"),
    square: "border border-primary bg-primary/25",
  },
  { title: "Planned", items: MILESTONES.filter((m) => m.status === "planned"), square: "bg-muted-foreground/45" },
  {
    title: "Exploring",
    items: MILESTONES.filter((m) => m.status === "exploring"),
    square: "border border-dashed border-muted-foreground/60",
  },
]

/** The roadmap as squares: one per milestone, filled as far as it has come. */
export function RoadmapDots() {
  return (
    <div className="cells">
      {KINDS.map((k) => (
        <div key={k.title} className="grid gap-3 p-5 sm:grid-cols-[8rem_1fr]">
          <div className="flex items-baseline gap-2">
            <span className="label-mono">{k.title}</span>
            <span className="font-mono text-[11px] text-muted-foreground">{k.items.length}</span>
          </div>
          <div className="flex flex-wrap gap-x-4 gap-y-2">
            {k.items.map((i) => (
              <span key={i.slug} className="flex items-center gap-2 text-sm" title={i.description}>
                <span className={cn("size-2.5 shrink-0", k.square)} />
                {i.title}
              </span>
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}

/** A benchmark run as a square, colored by where it ran. */
export type LoggedRun = { id: string; date: string; target: string; workload: string; color: string }

export function RunLog({ runs }: { runs: LoggedRun[] }) {
  return (
    <div className="flex flex-col gap-4 rounded-md border bg-card p-6">
      <div className="flex flex-wrap gap-1.5">
        {runs.map((r) => (
          <span
            key={r.id}
            className="size-4"
            style={{ background: r.color }}
            title={`${r.date} · ${r.workload} · ${r.target}`}
          />
        ))}
      </div>
      <ol className="flex flex-col gap-1 font-mono text-[11.5px] text-muted-foreground">
        {runs.map((r) => (
          <li key={r.id} className="flex items-center gap-3">
            <span className="size-2 shrink-0" style={{ background: r.color }} />
            <span className="text-foreground">{r.date}</span>
            <span>{r.workload}</span>
            <span>{r.target}</span>
          </li>
        ))}
      </ol>
    </div>
  )
}

/** A divider: a short band of dot paper that fades out at both ends. */
export function DotRule() {
  return (
    <div
      aria-hidden
      className="h-[7px] w-full"
      style={{
        ...dotPaper("var(--background)"),
        maskImage: "linear-gradient(to right, transparent, black 20%, black 80%, transparent)",
      }}
    />
  )
}
