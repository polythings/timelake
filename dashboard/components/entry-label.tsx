"use client"

import { Popover } from "@base-ui/react/popover"
import { ArrowRight } from "lucide-react"

/**
 * An entry's name in a ranking (a system, or a system and its reader) that shows what it is on hover or tap, with a
 * link to its full description on the benchmark's page.
 */
export function EntryLabel({
  label,
  description,
  details,
  href,
  className,
}: {
  label: string
  description: string
  /** Extra lines under the description, e.g. the best repeat. */
  details?: string[]
  href?: string
  className?: string
}) {
  return (
    <Popover.Root>
      <Popover.Trigger
        openOnHover
        delay={150}
        className={`min-w-0 cursor-help truncate text-left underline decoration-muted-foreground/40 decoration-dotted underline-offset-4 ${className ?? ""}`}
      >
        {label}
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Positioner side="top" align="start" sideOffset={6} className="z-50">
          <Popover.Popup className="flex w-[min(20rem,calc(100vw-2rem))] flex-col gap-1.5 rounded-lg border bg-background p-3 text-xs shadow-sm">
            <span className="font-medium text-foreground">{label}</span>
            <span className="leading-relaxed text-muted-foreground">{description}</span>
            {details && details.length > 0 && (
              <span className="font-mono text-[11px] text-muted-foreground tabular-nums">{details.join(" · ")}</span>
            )}
            {href && (
              <a href={href} className="flex w-fit items-center gap-1 text-foreground hover:underline">
                Learn more <ArrowRight className="size-3" aria-hidden />
              </a>
            )}
          </Popover.Popup>
        </Popover.Positioner>
      </Popover.Portal>
    </Popover.Root>
  )
}
