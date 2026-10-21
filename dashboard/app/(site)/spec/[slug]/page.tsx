import type { Metadata } from "next"
import Link from "next/link"
import { notFound } from "next/navigation"
import { Suspense } from "react"

import { SpecAside, SpecMeta } from "@/components/spec/spec-page"
import { SpecSidebar } from "@/components/spec/spec-sidebar"
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
  return page ? { title: page.title, description: page.summary || undefined } : { title: "Spec" }
}

export default function SpecPage({ params }: Props) {
  return (
    <div className="mx-auto flex max-w-6xl gap-10 px-4 py-10">
      <SpecAside>
        <SpecSidebar />
      </SpecAside>
      <Suspense fallback={<div className="h-96 flex-1 animate-pulse rounded-xl bg-muted" />}>
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
  // Only the milestones and specs that exist, so the lists' commas fall between entries.
  const milestones = (spec?.milestones ?? []).map(milestoneBySlug).filter((m) => m !== undefined)
  const related = (spec?.related ?? []).map(specByNumber).filter((r) => r !== undefined)
  return (
    <div className="min-w-0 flex-1 space-y-8">
      <header className="space-y-3">
        {spec ? (
          <>
            <p className="text-sm text-muted-foreground">
              Spec {spec.number} · {spec.area}
            </p>
            <h1 className="text-3xl font-light tracking-tight">{spec.title}</h1>
            <SpecMeta status={spec.status} updated={spec.updated} path={`spec/${spec.slug}.md`} />
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
              {milestones.length > 0 && (
                <>
                  <dt className="font-medium text-foreground">Roadmap</dt>
                  <dd>
                    {milestones.map((m, i) => (
                      <span key={m.slug}>
                        {i > 0 && ", "}
                        <Link className={LINK} href={`/roadmap/${m.slug}`}>
                          {m.title}
                        </Link>
                      </span>
                    ))}
                  </dd>
                </>
              )}
              {related.length > 0 && (
                <>
                  <dt className="font-medium text-foreground">Related</dt>
                  <dd>
                    {related.map((r, i) => (
                      <span key={r.slug}>
                        {i > 0 && ", "}
                        <Link className={LINK} href={`/spec/${r.slug}`}>
                          {r.number}. {r.title}
                        </Link>
                      </span>
                    ))}
                  </dd>
                </>
              )}
            </dl>
          </>
        ) : (
          <>
            <p className="text-sm text-muted-foreground">Background</p>
            <h1 className="text-3xl font-light tracking-tight">{page?.title}</h1>
          </>
        )}
      </header>
      <article className="prose-docs min-w-0">
        <Content />
      </article>
    </div>
  )
}
