"use client"

import { cn } from "@/lib/utils"

/**
 * A choice of how to view the same results (by time or by requests, default or tuned), in the look of the site's
 * pill tabs. Tabs with a line under them choose which results (the storage); this changes how they're shown.
 */
export function Segmented<T extends string>({
  options,
  value,
  onChange,
  label,
  className,
}: {
  options: { value: T; label: string }[]
  value: T
  onChange: (value: T) => void
  /** What the choice is, for screen readers. */
  label: string
  className?: string
}) {
  return (
    <div
      role="group"
      aria-label={label}
      className={cn("inline-flex w-fit items-center rounded-lg bg-muted p-[3px] text-sm", className)}
    >
      {options.map((o) => (
        <button
          key={o.value}
          type="button"
          aria-pressed={value === o.value}
          onClick={() => onChange(o.value)}
          className={cn(
            "min-h-8 rounded-md border border-transparent px-2.5 font-medium whitespace-nowrap text-muted-foreground transition-colors hover:text-foreground focus-visible:ring-[3px] focus-visible:ring-ring/50 focus-visible:outline-none sm:min-h-7",
            value === o.value && "bg-background text-foreground shadow-sm dark:border-input dark:bg-input/30",
          )}
        >
          {o.label}
        </button>
      ))}
    </div>
  )
}
