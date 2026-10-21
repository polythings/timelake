"use client"

import { useEffect, useState } from "react"

import { StripChart } from "@/components/lab/strip-chart"

/**
 * The demo table, written with the CLI into dashboard/lab-lake and served by app/lab-lake in local development only.
 * Its metadata holds the absolute file:// paths it was written to (another checkout's), so reads rewrite everything up
 * to the `lab-lake` folder to this server.
 */
const TABLE = "demo.readings"
const WRITTEN_AT = /^file:\/\/.*?\/lab-lake(?=\/|$)/

type Readout = { loadMs: number; queryMs: number; requests: number; points: number }

/**
 * The strip chart, drawing real data: 10-minute averages of one sensor, read from the demo table in this browser with
 * @timelake/client (the WebAssembly reader). The readout says what the read took.
 */
export function LiveStripChart({ children }: { children?: React.ReactNode }) {
  const [data, setData] = useState<number[] | null>(null)
  const [readout, setReadout] = useState<Readout | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let live = true
    const run = async () => {
      const t0 = performance.now()
      // Loaded after the page: the client carries its WebAssembly inline.
      const { Lake } = await import("@timelake/client")
      const t1 = performance.now()
      let requests = 0
      const base = `${window.location.origin}/lab-lake`
      const lake = new Lake(base, {
        fetch: (input, init) => {
          requests++
          return fetch(input, init)
        },
        rewriteUrl: (url) => url.replace(WRITTEN_AT, base),
      })
      const rows = await lake.table(TABLE).bucket({ series: "pump-1", every: "10m", agg: { temp: ["avg"] } })
      const t2 = performance.now()
      if (!live) return
      const values = rows.flatMap((r) => (r.temp_avg == null ? [] : [r.temp_avg]))
      setData(values)
      setReadout({ loadMs: t1 - t0, queryMs: t2 - t1, requests, points: values.length })
    }
    run().catch((e: unknown) => live && setError(e instanceof Error ? e.message : String(e)))
    return () => {
      live = false
    }
  }, [])

  return (
    <StripChart data={data}>
      {children}
      <div className="absolute right-3 bottom-2 rounded-sm border bg-background/90 px-2 py-1 font-mono text-[10.5px] text-muted-foreground">
        {error
          ? `read failed: ${error}`
          : readout
            ? `${TABLE} · pump-1 · ${readout.points} ten-minute averages · read in this browser: ${readout.requests} requests, ${Math.round(readout.queryMs)} ms (reader loaded in ${Math.round(readout.loadMs)} ms)`
            : "loading the reader…"}
      </div>
    </StripChart>
  )
}
