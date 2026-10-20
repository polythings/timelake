"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"

import { StatusDot, StatusLegend } from "@/components/spec/status-badge"
import { AREAS, BACKGROUND, SPECS } from "@/lib/specs"
import { cn } from "@/lib/utils"

const ITEM =
  "flex items-center gap-2 rounded-md px-2 py-1 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"

/** Every spec by area, each with a mark for its status, then the background pages. */
export function SpecSidebar() {
  const pathname = usePathname()
  return (
    <nav className="space-y-5 text-sm" aria-label="Spec">
      <div className="space-y-1">
        <Link href="/roadmap" className="block px-2 text-muted-foreground hover:text-foreground">
          Roadmap
        </Link>
        <StatusLegend className="px-2 pt-2" />
      </div>
      {AREAS.map((area) => (
        <div key={area} className="space-y-1">
          <p className="px-2 text-xs font-medium tracking-wide text-muted-foreground uppercase">{area}</p>
          {SPECS.filter((s) => s.area === area).map((s) => (
            <Link
              key={s.slug}
              href={`/spec/${s.slug}`}
              className={cn(ITEM, pathname === `/spec/${s.slug}` && "bg-muted font-medium text-foreground")}
            >
              <StatusDot status={s.status} />
              <span className="min-w-0 truncate">
                {s.number}. {s.title}
              </span>
            </Link>
          ))}
        </div>
      ))}
      <div className="space-y-1">
        <p className="px-2 text-xs font-medium tracking-wide text-muted-foreground uppercase">Background</p>
        {BACKGROUND.map((b) => (
          <Link
            key={b.slug}
            href={`/spec/${b.slug}`}
            className={cn(ITEM, pathname === `/spec/${b.slug}` && "bg-muted font-medium text-foreground")}
          >
            <span className="min-w-0 truncate">{b.title}</span>
          </Link>
        ))}
      </div>
    </nav>
  )
}
