import { ArrowUpRight, ChevronLeft } from "lucide-react"
import type { Metadata } from "next"
import Link from "next/link"
import { notFound } from "next/navigation"
import { Suspense } from "react"

import { SpecSidebar } from "@/components/spec/spec-sidebar"
import { StatusBadge } from "@/components/spec/status-badge"
import { REPO_URL } from "@/lib/site"
import { SPEC_PAGES } from "@/lib/spec-pages"
import { BACKGROUND, milestoneBySlug, SPECS, specByNumber, versionsText } from "@/lib/specs"

type Props = { params: Promise<{ slug: string }> }

const LINK = "underline underline-offset-4 hover:text-foreground"

export function generateStaticParams() {
  return Object.keys(SPEC_PAGES).map((slug) => ({ slug }))
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { slug } = await params
  const spec = SPECS.find((s) => s.slug === slug)
  if (spec) return { title: `${spec.number}. ${spec.title}`, description: spec.summary }
  const page = BACKGROUND.find((b) => b.slug === slug)
  return page ? { title: page.title, description: page.summary || undefined } : { title: "Specs" }
}

export default function SpecPage({ params }: Props) {
  return (
    <div className="mx-auto flex max-w-6xl gap-10 px-4 py-10">
      <aside className="sticky top-20 hidden h-[calc(100svh-6rem)] w-60 shrink-0 overflow-y-auto md:block">
        <SpecSidebar />
      </aside>
      <Suspense fallback={<div className="h-96 max-w-3xl flex-1 animate-pulse rounded-xl bg-muted" />}>
        <SpecBody params={params} />
      </Suspense>
    </div>
  )
}

/** A spec's or background page's header and its Markdown. */
async function SpecBody({ params }: Props) {
  const { slug } = await params
  const load = SPEC_PAGES[slug]
  if (!load) notFound()
  const { default: Content } = await load()
  const spec = SPECS.find((s) => s.slug === slug)
  const page = BACKGROUND.find((b) => b.slug === slug)
  return (
    <div className="max-w-3xl min-w-0 flex-1 space-y-8">
      <header className="space-y-3">
        <Link href="/spec" className="flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground">
          <ChevronLeft className="size-4" />
          All specs
        </Link>
        {spec ? (
          <>
            <p className="text-sm text-muted-foreground">
              Spec {spec.number} · {spec.area}
            </p>
            <h1 className="text-3xl font-semibold tracking-tight">{spec.title}</h1>
            <div className="flex flex-wrap items-center gap-x-3 gap-y-2 text-sm text-muted-foreground">
              <StatusBadge status={spec.status} />
              <span className="tabular-nums">Updated {spec.updated}</span>
              <a
                className="inline-flex items-center gap-0.5 hover:text-foreground"
                href={`${REPO_URL}/blob/main/spec/${spec.slug}.md`}
              >
                View on GitHub
                <ArrowUpRight className="size-3.5" />
              </a>
            </div>
            <dl className="grid gap-x-4 gap-y-1 text-sm text-muted-foreground sm:grid-cols-[6rem_1fr]">
              <dt className="font-medium text-foreground">Versions</dt>
              <dd className="tabular-nums">
                {spec.versions.length > 0 ? (
                  <Link className={LINK} href="/docs/changelog">
                    {versionsText(spec.versions)}
                  </Link>
                ) : (
                  "Not released yet"
                )}
              </dd>
              <dt className="font-medium text-foreground">Roadmap</dt>
              <dd>
                {spec.milestones.map((name, i) => {
                  const m = milestoneBySlug(name)
                  return (
                    m && (
                      <span key={name}>
                        {i > 0 && ", "}
                        <Link className={LINK} href={`/roadmap/${m.slug}`}>
                          {m.title}
                        </Link>
                      </span>
                    )
                  )
                })}
              </dd>
              {spec.related.length > 0 && (
                <>
                  <dt className="font-medium text-foreground">Related</dt>
                  <dd>
                    {spec.related.map((n, i) => {
                      const r = specByNumber(n)
                      return (
                        r && (
                          <span key={n}>
                            {i > 0 && ", "}
                            <Link className={LINK} href={`/spec/${r.slug}`}>
                              {r.number}. {r.title}
                            </Link>
                          </span>
                        )
                      )
                    })}
                  </dd>
                </>
              )}
            </dl>
          </>
        ) : (
          <>
            <p className="text-sm text-muted-foreground">Background</p>
            <h1 className="text-3xl font-semibold tracking-tight">{page?.title}</h1>
          </>
        )}
      </header>
      <article className="prose-docs min-w-0">
        <Content />
      </article>
    </div>
  )
}
