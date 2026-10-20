import { ArrowRight, ArrowUpRight } from "lucide-react"
import type { Metadata } from "next"
import Link from "next/link"
import { notFound } from "next/navigation"

import { BackLink } from "@/components/back-link"
import { Badge } from "@/components/ui/badge"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { BENCHMARKS, benchmark, type Section } from "@/lib/benchmark-catalog"

type Props = { params: Promise<{ id: string }> }

export function generateStaticParams() {
  return BENCHMARKS.filter((b) => b.methodology).map((b) => ({ id: b.id }))
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const b = benchmark((await params).id)
  return { title: b ? `${b.name} methodology` : "Benchmarks" }
}

/** `code` spans in catalog text. */
function Text({ children }: { children: string }) {
  let offset = 0
  return children.split(/(`[^`]+`)/).map((part) => {
    const key = offset // where the part starts: unique within the string
    offset += part.length
    return part.startsWith("`") ? (
      <code key={key} className="font-mono text-[0.9em]">
        {part.slice(1, -1)}
      </code>
    ) : (
      <span key={key}>{part}</span>
    )
  })
}

function SectionBlock({ section }: { section: Section }) {
  return (
    <section className="space-y-3">
      <h2 className="text-lg font-semibold tracking-tight">{section.title}</h2>
      {section.text?.map((t) => (
        <p key={t} className="text-muted-foreground">
          <Text>{t}</Text>
        </p>
      ))}
      {section.items && (
        <ul className="ml-5 list-disc space-y-1.5 text-muted-foreground">
          {section.items.map((t) => (
            <li key={t}>
              <Text>{t}</Text>
            </li>
          ))}
        </ul>
      )}
      {section.table && (
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                {section.table.head.map((h) => (
                  <TableHead key={h}>{h}</TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {section.table.rows.map((row) => (
                <TableRow key={row.join("|")}>
                  {row.map((cell, i) => (
                    <TableCell
                      key={section.table?.head[i]}
                      className={i === 0 ? "font-medium" : "min-w-40 whitespace-normal text-muted-foreground"}
                    >
                      {cell}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </section>
  )
}

export default async function MethodologyPage({ params }: Props) {
  const b = benchmark((await params).id)
  if (!b?.methodology) notFound()
  const results = b.status === "running"
  return (
    <div className="mx-auto max-w-3xl space-y-10 px-4 py-12">
      <header className="space-y-3">
        <BackLink href={results ? `/benchmarks/${b.id}` : "/benchmarks"}>
          {results ? `${b.name} results` : "Benchmarks"}
        </BackLink>
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="text-3xl font-semibold tracking-tight">{b.name}: methodology</h1>
          {!results && <Badge variant="outline">Planned</Badge>}
        </div>
        <p className="text-muted-foreground">{b.summary}</p>
        <nav className="flex flex-wrap gap-x-4 gap-y-1 text-sm">
          {b.origin && (
            <a href={b.origin.url} className="flex items-center gap-1 hover:underline">
              {b.origin.label} <ArrowUpRight className="size-3.5" />
            </a>
          )}
          <Link
            href="/benchmarks/timebench"
            className="flex items-center gap-1 text-muted-foreground hover:text-foreground hover:underline"
          >
            How timebench runs every benchmark <ArrowRight className="size-3.5" />
          </Link>
        </nav>
      </header>
      {!results && (
        <p className="rounded-lg border border-dashed p-4 text-sm text-muted-foreground">
          Not run yet. {b.why} This page is the plan.
        </p>
      )}
      {b.provenance && (
        <SectionBlock section={{ title: results ? "What we ran" : "What we'll run", text: [b.provenance] }} />
      )}
      {b.methodology.map((section) => (
        <SectionBlock key={section.title} section={section} />
      ))}
    </div>
  )
}
