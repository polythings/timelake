import type { Metadata } from "next"
import Link from "next/link"

import { SpecList } from "@/components/spec/spec-list"
import { REPO_URL } from "@/lib/site"
import { BACKGROUND, STATUS_LABEL, STATUS_MEANING, type SpecStatus } from "@/lib/specs"

export const metadata: Metadata = {
  title: "Specs",
  description: "Timelake's design, one numbered spec per feature, with what is built, in progress, and planned.",
}

export default function SpecsPage() {
  return (
    <div className="mx-auto max-w-6xl space-y-10 px-4 py-12">
      <header className="max-w-3xl space-y-3">
        <h1 className="text-3xl font-semibold tracking-tight">Specs</h1>
        <p className="text-muted-foreground">
          Timelake&apos;s design, one numbered spec per feature: what it does and why, what was measured to decide it,
          how far it is built, and the releases that changed it. The specs live in the repository&apos;s{" "}
          <a className="underline underline-offset-4" href={`${REPO_URL}/tree/main/spec`}>
            spec directory
          </a>
          ; open questions and tasks are{" "}
          <a className="underline underline-offset-4" href={`${REPO_URL}/issues`}>
            GitHub issues
          </a>
          . The{" "}
          <Link className="underline underline-offset-4" href="/roadmap">
            roadmap
          </Link>{" "}
          groups them into milestones.
        </p>
        <dl className="grid gap-x-4 gap-y-1 text-sm sm:grid-cols-[7rem_1fr]">
          {(Object.keys(STATUS_LABEL) as SpecStatus[]).map((s) => (
            <div key={s} className="contents">
              <dt className="font-medium">{STATUS_LABEL[s]}</dt>
              <dd className="text-muted-foreground">{STATUS_MEANING[s]}</dd>
            </div>
          ))}
        </dl>
      </header>
      <SpecList />
      <section className="space-y-3">
        <h2 className="text-xl font-medium tracking-tight">Background</h2>
        <ul className="divide-y border-y">
          {BACKGROUND.map((b) => (
            <li key={b.slug}>
              <Link
                href={`/spec/${b.slug}`}
                className="block space-y-1 py-4 transition-colors hover:bg-muted/40 sm:pl-16"
              >
                <span className="block font-medium">{b.title}</span>
                {b.summary && <span className="block text-sm text-muted-foreground">{b.summary}</span>}
              </Link>
            </li>
          ))}
        </ul>
      </section>
    </div>
  )
}
