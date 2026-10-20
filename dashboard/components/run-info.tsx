"use client"

import { Popover } from "@base-ui/react/popover"
import { Info } from "lucide-react"

import type { RunInfo } from "@/lib/benchmarks"
import { REPO_URL } from "@/lib/site"

/** An ⓘ that shows where a run comes from on hover (or tap): date, machine, commit, and the version of everything. */
export function RunInfoButton({ info, label }: { info: RunInfo; label: string }) {
  return (
    <Popover.Root>
      <Popover.Trigger
        openOnHover
        delay={100}
        aria-label={`About the ${label} run`}
        className="inline-flex size-5 items-center justify-center rounded-full text-muted-foreground hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none"
      >
        <Info className="size-3.5" aria-hidden />
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Positioner side="bottom" align="start" sideOffset={6} className="z-50">
          <Popover.Popup className="flex w-[min(24rem,calc(100vw-2rem))] flex-col gap-2 rounded-lg border bg-background p-3 text-xs shadow-sm">
            <div className="flex flex-wrap gap-x-3 gap-y-0.5">
              <span className="font-mono tabular-nums">{info.date}</span>
              <span>{info.machine}</span>
              <a className="font-mono underline-offset-4 hover:underline" href={`${REPO_URL}/commit/${info.commit}`}>
                {info.commit.slice(0, 7)}
              </a>
              {info.imported && <span className="text-muted-foreground">from the v0 scripts</span>}
            </div>
            <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-0.5 font-mono text-muted-foreground">
              {info.versions.map(([name, version]) => (
                <div key={name} className="contents">
                  <dt>{name}</dt>
                  <dd className="text-right text-foreground tabular-nums">{version}</dd>
                </div>
              ))}
            </dl>
          </Popover.Popup>
        </Popover.Positioner>
      </Popover.Portal>
    </Popover.Root>
  )
}
