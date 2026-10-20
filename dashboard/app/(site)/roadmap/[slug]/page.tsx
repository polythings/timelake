import { ArrowUpRight, ChevronLeft } from "lucide-react"
import type { Metadata } from "next"
import Link from "next/link"
import { notFound } from "next/navigation"
import { Suspense } from "react"

import { StatusBadge } from "@/components/spec/status-badge"
import { REPO_URL } from "@/lib/site"
import { MILESTONE_PAGES } from "@/lib/spec-pages"
import {
  MILESTONE_STATUS_LABEL,
  milestoneBySlug,
  MILESTONES,
  type MilestoneStatus,
  specByNumber,
  versionsText,
} from "@/lib/specs"
import { cn } from "@/lib/utils"

type Props = { params: Promise<{ slug: string }> }

export function generateStaticParams() {
  return Object.keys(MILESTONE_PAGES).map((slug) => ({ slug }))
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { slug } = await params
  const m = milestoneBySlug(slug)
  return m ? { title: m.title, description: m.description } : { title: "Roadmap" }
}

export default function MilestonePage({ params }: Props) {
  return (
    <div className="mx-auto flex max-w-6xl gap-10 px-4 py-10">
      <Suspense fallback={<div className="h-96 max-w-3xl flex-1 animate-pulse rounded-xl bg-muted" />}>
        <MilestoneBody params={params} />
      </Suspense>
    </div>
  )
}

/** Every milestone by status, the current one marked. */
function MilestoneNav({ current }: { current: string }) {
  return (
    <nav className="space-y-5 text-sm" aria-label="Milestones">
      <Link href="/roadmap" className="block px-2 font-medium">
        Roadmap
      </Link>
      {(Object.keys(MILESTONE_STATUS_LABEL) as MilestoneStatus[]).map((status) => (
        <div key={status} className="space-y-1">
          <p className="px-2 text-xs font-medium tracking-wide text-muted-foreground uppercase">
            {MILESTONE_STATUS_LABEL[status]}
          </p>
          {MILESTONES.filter((m) => m.status === status)
            .toSorted((x, y) => (status === "done" ? y.number - x.number : x.number - y.number))
            .map((m) => (
              <Link
                key={m.slug}
                href={`/roadmap/${m.slug}`}
                className={cn(
                  "block truncate rounded-md px-2 py-1 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground",
                  m.slug === current && "bg-muted font-medium text-foreground",
                )}
              >
                {m.title}
              </Link>
            ))}
        </div>
      ))}
    </nav>
  )
}

/** The milestone's header, its specs, and its Markdown. */
async function MilestoneBody({ params }: Props) {
  const { slug } = await params
  const load = MILESTONE_PAGES[slug]
  const m = milestoneBySlug(slug)
  if (!load || !m) notFound()
  const { default: Content } = await load()
  const specs = m.specs.map(specByNumber).filter((s) => s !== undefined)
  return (
    <>
      <aside className="sticky top-20 hidden h-[calc(100svh-6rem)] w-60 shrink-0 overflow-y-auto md:block">
        <MilestoneNav current={slug} />
      </aside>
      <div className="max-w-3xl min-w-0 flex-1 space-y-8">
        <header className="space-y-3">
          <Link href="/roadmap" className="flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground">
            <ChevronLeft className="size-4" />
            Roadmap
          </Link>
          <p className="text-sm text-muted-foreground">Milestone</p>
          <h1 className="text-3xl font-light tracking-tight">{m.title}</h1>
          <div className="flex flex-wrap items-center gap-x-3 gap-y-2 text-sm text-muted-foreground">
            <StatusBadge status={m.status} />
            {m.versions && <span className="tabular-nums">{versionsText(m.versions)}</span>}
            <span className="tabular-nums">Updated {m.updated}</span>
            <a
              className="inline-flex items-center gap-0.5 hover:text-foreground"
              href={`${REPO_URL}/blob/main/spec/milestones/${m.slug}.md`}
            >
              View on GitHub
              <ArrowUpRight className="size-3.5" />
            </a>
          </div>
          <p className="text-muted-foreground">{m.description}</p>
        </header>
        <section className="space-y-3">
          <h2 className="text-xl font-medium tracking-tight">Specs</h2>
          <ul className="divide-y border-y">
            {specs.map((s) => (
              <li key={s.slug}>
                <Link
                  href={`/spec/${s.slug}`}
                  className="grid gap-x-4 gap-y-1 py-3 transition-colors hover:bg-muted/40 sm:grid-cols-[3rem_1fr_auto]"
                >
                  <span className="font-mono text-sm text-muted-foreground tabular-nums">{s.number}</span>
                  <span className="min-w-0 font-medium">{s.title}</span>
                  <span className="flex flex-wrap items-center gap-x-3 gap-y-1 sm:justify-end">
                    <StatusBadge status={s.status} />
                    {s.versions.length > 0 && (
                      <span className="text-xs text-muted-foreground tabular-nums">{versionsText(s.versions)}</span>
                    )}
                  </span>
                </Link>
              </li>
            ))}
          </ul>
        </section>
        <article className="prose-docs min-w-0">
          <Content />
        </article>
      </div>
    </>
  )
}
