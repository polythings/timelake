"use client"

import Link from "next/link"
import { useState } from "react"

import { StatusBadge } from "@/components/spec/status-badge"
import { Button } from "@/components/ui/button"
import { AREAS, SPECS, STATUS_LABEL, type SpecStatus } from "@/lib/specs"

const STATUSES = (Object.keys(STATUS_LABEL) as SpecStatus[]).filter((s) => SPECS.some((spec) => spec.status === s))

/** Every spec by area, filtered by status. */
export function SpecList() {
  const [status, setStatus] = useState<SpecStatus | null>(null)
  const shown = SPECS.filter((s) => status === null || s.status === status)
  return (
    <div className="space-y-10">
      <div className="flex flex-wrap gap-2" role="group" aria-label="Filter by status">
        <Button size="sm" variant={status === null ? "default" : "outline"} onClick={() => setStatus(null)}>
          All {SPECS.length}
        </Button>
        {STATUSES.map((s) => (
          <Button key={s} size="sm" variant={status === s ? "default" : "outline"} onClick={() => setStatus(s)}>
            {STATUS_LABEL[s]} {SPECS.filter((spec) => spec.status === s).length}
          </Button>
        ))}
      </div>
      {AREAS.map((area) => {
        const specs = shown.filter((s) => s.area === area)
        if (specs.length === 0) return null
        return (
          <section key={area} className="space-y-3">
            <h2 className="text-xl font-medium tracking-tight">{area}</h2>
            <ul className="divide-y border-y">
              {specs.map((s) => (
                <li key={s.slug}>
                  <Link
                    href={`/spec/${s.slug}`}
                    className="grid gap-x-4 gap-y-1 py-4 transition-colors hover:bg-muted/40 sm:grid-cols-[3rem_1fr_auto]"
                  >
                    <span className="font-mono text-sm text-muted-foreground tabular-nums">{s.number}</span>
                    <span className="min-w-0 space-y-1">
                      <span className="block font-medium">{s.title}</span>
                      <span className="block text-sm text-muted-foreground">{s.summary}</span>
                    </span>
                    <span className="flex items-start gap-3 sm:flex-col sm:items-end">
                      <StatusBadge status={s.status} />
                      <span className="text-xs text-muted-foreground tabular-nums">{s.updated}</span>
                    </span>
                  </Link>
                </li>
              ))}
            </ul>
          </section>
        )
      })}
    </div>
  )
}
