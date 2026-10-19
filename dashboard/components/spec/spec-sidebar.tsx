"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"

import { AREAS, SPECS, type SpecStatus } from "@/lib/specs"
import { cn } from "@/lib/utils"

const DOT: Record<SpecStatus, string> = {
  done: "bg-foreground",
  "in-progress": "bg-foreground/50",
  planned: "border border-foreground/40",
  dropped: "bg-transparent",
}

/** Every spec by area, each with a mark for its status. */
export function SpecSidebar() {
  const pathname = usePathname()
  return (
    <nav className="space-y-5 text-sm" aria-label="Specs">
      <Link href="/spec" className="block px-2 font-medium">
        All specs
      </Link>
      {AREAS.map((area) => (
        <div key={area} className="space-y-1">
          <p className="px-2 text-xs font-medium tracking-wide text-muted-foreground uppercase">{area}</p>
          {SPECS.filter((s) => s.area === area).map((s) => (
            <Link
              key={s.slug}
              href={`/spec/${s.slug}`}
              className={cn(
                "flex items-center gap-2 rounded-md px-2 py-1 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground",
                pathname === `/spec/${s.slug}` && "bg-muted font-medium text-foreground",
              )}
            >
              <span className={cn("size-2 shrink-0", DOT[s.status])} aria-hidden />
              <span className="min-w-0 truncate">
                {s.number}. {s.title}
              </span>
            </Link>
          ))}
        </div>
      ))}
    </nav>
  )
}
