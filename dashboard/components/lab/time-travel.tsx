"use client"

import { useState } from "react"

import { formatNumber } from "@/lib/format"

/** One snapshot from the table's metadata: its version, what the commit did, and the table's totals after it. */
export type Snapshot = { version: number; id: string; operation: string; added: number; rows: number; files: number }

/**
 * A slider over a table's snapshots: every commit is a version of the table that stays readable until it expires.
 * The numbers come from the demo table's Iceberg metadata.
 */
export function TimeTravel({ snapshots, days }: { snapshots: Snapshot[]; days: string[] }) {
  const [at, setAt] = useState(snapshots.length - 1)
  const s = snapshots[at]
  const most = Math.max(...snapshots.map((x) => x.rows))
  if (!s) return null
  return (
    <div className="cells lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
      <div className="flex flex-col gap-5 p-6">
        <div className="flex items-end gap-1.5" style={{ height: 120 }}>
          {snapshots.map((x, i) => (
            <button
              key={x.id}
              type="button"
              aria-label={`Version ${x.version}`}
              onClick={() => setAt(i)}
              className={`flex-1 rounded-t-sm transition-colors ${i === at ? "bg-primary" : i < at ? "bg-chart-2/70" : "bg-border"}`}
              style={{ height: `${(x.rows / most) * 100}%` }}
            />
          ))}
        </div>
        <input
          type="range"
          min={0}
          max={snapshots.length - 1}
          value={at}
          onChange={(e) => setAt(Number(e.target.value))}
          aria-label="Table version"
          className="w-full accent-[var(--primary)]"
        />
        <dl className="grid grid-cols-2 gap-x-6 gap-y-2 font-mono text-[12px] sm:grid-cols-4">
          {[
            ["version", `v${s.version}`],
            ["commit", s.operation],
            ["rows", formatNumber(s.rows)],
            ["files", String(s.files)],
          ].map(([k, v]) => (
            <div key={k} className="flex flex-col gap-0.5">
              <dt className="text-[10.5px] tracking-[0.1em] text-muted-foreground uppercase">{k}</dt>
              <dd className="text-foreground">{v}</dd>
            </div>
          ))}
        </dl>
      </div>
      <pre className="overflow-x-auto p-6 font-mono text-[12px] leading-[1.75]">
        <span className="block text-muted-foreground">{`$ timelake rollback demo.readings ${s.id}`}</span>
        <span className="block text-foreground">demo/readings/data/</span>
        {days.map((d, i) => (
          <span key={d} className={`block transition-colors ${i < s.files ? "text-foreground/80" : "text-border"}`}>
            {`  time_day=${d}/`.padEnd(28)}
            <span className="text-muted-foreground">
              {i < s.files ? (i === s.files - 1 ? "← added in this version" : "") : "not yet written"}
            </span>
          </span>
        ))}
      </pre>
    </div>
  )
}
