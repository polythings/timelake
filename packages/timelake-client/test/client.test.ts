// Reads a generated warehouse over HTTP and checks the results against the Rust engine.
// Needs the CLI: `cargo build -p timelake-cli` at the repo root (or set TIMELAKE_CLI).
import { afterAll, beforeAll, expect, test } from "bun:test"
import { readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import path from "node:path"

import { z } from "zod"

import { Lake, readerVersion, zodSource, type AggOp, type Fetch } from "../src/index"
import { ParquetFile } from "../src/wasm/timelake_wasm.js"
import { buildFixture, POINTS, SERIES, START, type Fixture } from "./fixture"

let fx: Fixture
let lake: Lake

beforeAll(() => {
  fx = buildFixture()
  lake = new Lake(fx.url)
})
afterAll(() => fx?.close())

const Reading = z.object({
  time: z.coerce.date(),
  tags_id: z.number().int(),
  velocity: z.number().nullable(),
  name: z.string(),
})

const byTag = (a: { tags_id: number }, b: { tags_id: number }) => a.tags_id - b.tags_id

test("wasm loads", () => {
  // The WASM crate and the npm package share the project's one version.
  const pkg = JSON.parse(readFileSync(path.join(import.meta.dir, "../package.json"), "utf8")) as { version: string }
  expect(readerVersion()).toBe(pkg.version)
})

test("lists tables and reads info", async () => {
  expect(await lake.tables()).toEqual(["iot.readings", "iot.tags", "iot.unindexed"])
  const info = await lake.table("iot.readings").info()
  expect(info.timeColumn).toBe("time")
  expect(info.seriesKey).toEqual(["tags_id"])
  expect(info.chunk).toBe("hour")
  expect(info.files.length).toBe(24)
  expect(info.files.reduce((s, f) => s + f.records, 0)).toBe(SERIES * POINTS)
  // Hour partitions, as hours since the epoch.
  const first = START / 3_600_000
  expect(info.files.map((f) => f.partition).toSorted()).toEqual(Array.from({ length: 24 }, (_, i) => first + i))
})

test("finds a commit the version hint doesn't mention yet", async () => {
  const hint = path.join(fx.dir, "lake/iot/readings/metadata/version-hint.text")
  const version = Number(readFileSync(hint, "utf8"))
  writeFileSync(hint, String(version - 1))
  try {
    expect((await new Lake(fx.url).table("iot.readings").info()).version).toBe(version)
  } finally {
    writeFileSync(hint, String(version))
  }
})

test("latest matches the engine", async () => {
  const t = lake.table("iot.readings", Reading)
  const rows = await t.latest()
  // The last-value index came with the table's head: no request beyond loading the table (the
  // head, the version hint, and a HEAD request for a newer version).
  expect(t.lastStats.requests).toBe(3)
  const expected = fx.sql(
    "SELECT tags_id, velocity FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY tags_id ORDER BY time DESC) rn FROM iot.readings) WHERE rn = 1 ORDER BY tags_id",
  )
  expect(rows.length).toBe(SERIES)
  expect(rows[0]?.time).toBeInstanceOf(Date)
  expect(rows.toSorted(byTag).map((r) => ({ tags_id: r.tags_id, velocity: r.velocity }))).toEqual(
    expected as { tags_id: number; velocity: number | null }[],
  )
  const some = await t.latest({ series: [3, 7] })
  expect(some.map((r) => r.tags_id).toSorted()).toEqual([3, 7])
})

test("reads tables from their head, others through manifests", async () => {
  expect((await lake.table("iot.readings").info()).manifests).toBeNull()
  expect((await lake.table("iot.unindexed").info()).manifests).toBe(1)
})

/** Creates `iot.<table>` with one row of series 1: `velocity` at minute 0. */
function createSmall(table: string, velocity: number) {
  const csv = path.join(fx.dir, `${table}.csv`)
  writeFileSync(csv, `time,tags_id,velocity\n2026-01-01T00:00:00Z,1,${velocity}\n`)
  const schema = ["--schema", "time:timestamptz,tags_id:int,velocity:double"]
  fx.cli("import", `iot.${table}`, csv, ...schema, "--time-column", "time", "--series-key", "tags_id")
}

/** Appends a row of series 1 to `iot.<table>`: `velocity` at `minute`. */
function appendSmall(table: string, minute: number, velocity: number) {
  const csv = path.join(fx.dir, `${table}.csv`)
  const time = new Date(START + minute * 60_000).toISOString()
  writeFileSync(csv, `time,tags_id,velocity\n${time},1,${velocity}\n`)
  fx.cli("append", `iot.${table}`, csv)
}

const metadataDir = (table: string) => path.join(fx.dir, "lake/iot", table, "metadata")

/** Committed versions of `iot.<table>`, from its metadata files. */
function versions(table: string): number[] {
  return readdirSync(metadataDir(table))
    .map((f) => /^v(\d+)\.metadata\.json$/.exec(f)?.[1])
    .filter((v) => v !== undefined)
    .map(Number)
    .toSorted((a, b) => a - b)
}

test("notices a version newer than the head", async () => {
  createSmall("moving", 1.5)
  const meta = metadataDir("moving")
  const [head, hint] = [
    readFileSync(path.join(meta, "timelake-head")),
    readFileSync(path.join(meta, "version-hint.text")),
  ]
  appendSmall("moving", 5, 2.5)
  // As if the newest commit's head and hint were not written yet.
  writeFileSync(path.join(meta, "timelake-head"), head)
  writeFileSync(path.join(meta, "version-hint.text"), hint)
  const t = new Lake(fx.url).table("iot.moving")
  expect((await t.latest()).map((r) => r.velocity)).toEqual([2.5])
  expect((await t.info()).version).toBe(Number(hint) + 1)
})

test("a handle many versions behind reads the newest version once cacheMs is over", async () => {
  createSmall("behind", 0)
  const expiring = new Lake(fx.url, { cacheMs: 0 }).table("iot.behind")
  const cached = new Lake(fx.url, { cacheMs: 60_000 }).table("iot.behind")
  const first = (await expiring.info()).version
  expect((await cached.info()).version).toBe(first)
  for (let i = 1; i <= 5; i++) appendSmall("behind", i, i)
  const newest = Math.max(...versions("behind"))
  expect(newest).toBeGreaterThanOrEqual(first + 5)
  expect((await expiring.latest()).map((r) => r.velocity)).toEqual([5])
  expect((await expiring.info()).version).toBe(newest)
  // Within cacheMs, the loaded version is reused.
  expect((await cached.latest()).map((r) => r.velocity)).toEqual([0])
  expect((await cached.info()).version).toBe(first)
})

test("a handle behind the versions maintenance deleted reads the newest version", async () => {
  createSmall("expired", 0)
  const t = new Lake(fx.url, { cacheMs: 0 }).table("iot.expired")
  const first = (await t.info()).version
  for (let i = 1; i <= 4; i++) appendSmall("expired", i, i)
  // As maintenance does: delete the oldest metadata files, the loaded version's included.
  for (const version of versions("expired").filter((v) => v <= first + 1)) {
    rmSync(path.join(metadataDir("expired"), `v${version}.metadata.json`))
  }
  expect((await t.latest()).map((r) => r.velocity)).toEqual([4])
  expect((await t.info()).version).toBe(Math.max(...versions("expired")))
})

test("a missing object answered with 403 reads like a 404", async () => {
  // Public S3 buckets without s3:ListBucket answer 403 for a key that doesn't exist.
  const forbidden: Fetch = async (url, init) => {
    const r = await fetch(url, init)
    return r.status === 404 ? new Response(null, { status: 403 }) : r
  }
  const t = new Lake(fx.url, { fetch: forbidden, cacheMs: 0 }).table("iot.readings")
  expect(await t.latest()).toHaveLength(SERIES)
  expect(await t.latest()).toHaveLength(SERIES) // after cacheMs: checks for a newer version
  expect((await new Lake(fx.url, { fetch: forbidden }).table("iot.unindexed").info()).manifests).toBe(1)
  await expect(new Lake(fx.url, { fetch: forbidden }).table("iot.missing").info()).rejects.toThrow("table not found")
})

test("lastStats counts the requests that load the table", async () => {
  const t = new Lake(fx.url).table("iot.unindexed")
  await t.range({ series: 7, limit: 1 })
  const cold = t.lastStats
  await t.range({ series: 7, limit: 1 })
  const warm = t.lastStats
  // Loading added the head and the hint (both absent), the metadata, the next version's (absent),
  // the manifest list and the manifest.
  expect(cold.requests).toBe(warm.requests + 6)
  expect(cold.bytes).toBeGreaterThan(warm.bytes)
  expect(warm.files).toBe(cold.files)
})

test("reads from servers that ignore Range", async () => {
  const noRange: Fetch = (url, init) => fetch(url, { ...init, headers: undefined })
  const t = new Lake(fx.url, { fetch: noRange }).table("iot.unindexed", Reading)
  const ranged = lake.table("iot.unindexed", Reading)
  for (let i = 0; i < 2; i++) expect(await t.range({ series: 7 })).toEqual(await ranged.range({ series: 7 }))
  // Each request got the whole file.
  expect(t.lastStats.bytes).toBeGreaterThan(ranged.lastStats.bytes)
})

test("a failed query frees the files it opened", async () => {
  const planned: ParquetFile[] = []
  const freed = new Set<ParquetFile>()
  const { plan, free } = ParquetFile.prototype
  ParquetFile.prototype.plan = function (this: ParquetFile, query: unknown): unknown {
    planned.push(this)
    return plan.call(this, query)
  }
  ParquetFile.prototype.free = function (this: ParquetFile): void {
    freed.add(this)
    free.call(this)
  }
  try {
    let ranges = 0
    const failing: Fetch = (url, init) => {
      if (init?.headers && ++ranges === 5) return Promise.reject(new Error("connection reset"))
      return fetch(url, init)
    }
    await expect(new Lake(fx.url, { fetch: failing }).table("iot.readings").range()).rejects.toThrow("connection reset")
    await Bun.sleep(100) // let any fetches still in flight finish
    expect(planned.length).toBeGreaterThan(1)
    expect(planned.filter((f) => !freed.has(f))).toHaveLength(0)
  } finally {
    Object.assign(ParquetFile.prototype, { plan, free })
  }
})

test("latest scans tables without a last-value index (B12)", async () => {
  const t = lake.table("iot.unindexed", Reading)
  expect((await t.info()).lastValueIndex).toBeNull()
  const rows = await t.latest()
  expect(t.lastStats.files).toBe(24)
  const expected = fx.sql(
    "SELECT tags_id, time, velocity FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY tags_id ORDER BY time DESC) rn FROM iot.unindexed) WHERE rn = 1 ORDER BY tags_id",
  )
  const got = rows.toSorted(byTag).map((r) => ({ ...r, time: r.time.toISOString().replace(".000Z", "Z") }))
  expect(got.map(({ tags_id, time, velocity }) => ({ tags_id, time, velocity }))).toEqual(
    expected as { tags_id: number; time: string; velocity: number | null }[],
  )
  const indexed = await lake.table("iot.readings", Reading).latest({ series: [3, 7] })
  const scanned = await t.latest({ series: [3, 7] })
  expect(scanned.toSorted(byTag)).toEqual(indexed.toSorted(byTag))
})

test("range matches the engine and prunes files", async () => {
  const t = lake.table("iot.readings", Reading)
  const from = "2026-01-01T12:00:00Z"
  const to = "2026-01-01T13:00:00Z"
  const rows = await t.range({ series: 7, from, to })
  const expected = fx.sql(
    `SELECT time, velocity FROM iot.readings WHERE tags_id = 7 AND time >= TIMESTAMP '2026-01-01 12:00:00' AND time < TIMESTAMP '2026-01-01 13:00:00' ORDER BY time`,
  )
  expect(rows.length).toBe(12)
  expect(rows.map((r) => r.velocity)).toEqual(expected.map((r) => r.velocity) as (number | null)[])
  expect(t.lastStats.files).toBe(1) // one hour partition
  // Times without an offset are UTC, wherever the client runs.
  expect(await t.range({ series: 7, from: "2026-01-01T12:00:00", to: "2026-01-01 13:00" })).toEqual(rows)
  const desc = await t.range({ series: 7, limit: 1, order: "desc", columns: ["time", "velocity"] })
  expect(desc.length).toBe(1)
  expect(desc[0]?.time.toISOString()).toBe("2026-01-01T23:55:00.000Z")
})

test("series values an int column can't hold match nothing (B3)", async () => {
  const t = lake.table("iot.readings")
  expect(await t.range({ series: 2 ** 32 + 7 })).toEqual([])
  expect(await t.range({ series: 7.5 })).toEqual([])
  expect(await t.range({ series: "7", limit: 1 })).toHaveLength(1)
})

test("queries over more files than the concurrency limit finish (B1)", async () => {
  const [{ n }] = fx.sql("SELECT count(*) AS n FROM iot.readings") as [{ n: number }]
  for (const concurrency of [1, 2, 16]) {
    const t = new Lake(fx.url, { concurrency }).table("iot.readings")
    expect(await t.range()).toHaveLength(n)
    expect(t.lastStats.files).toBe(24)
  }
})

test("bucket aggregates match the engine", async () => {
  const t = lake.table("iot.readings")
  const rows = await t.bucket({ series: 7, every: "1h", agg: { velocity: ["avg", "max"], "*": ["count"] } })
  const expected = fx.sql(
    "SELECT date_bin(INTERVAL '1 hour', time) AS bucket, avg(velocity) AS velocity_avg, max(velocity) AS velocity_max, count(*) AS count FROM iot.readings WHERE tags_id = 7 GROUP BY 1 ORDER BY 1",
  )
  expect(rows.length).toBe(24)
  expect(rows.length).toBe(expected.length)
  rows.forEach((r, i) => {
    const e = expected[i] ?? {}
    expect(r.bucket.toISOString().replace(".000Z", "Z")).toBe(String(e.bucket))
    expect(r.velocity_avg).toBeCloseTo(Number(e.velocity_avg), 9)
    expect(r.velocity_max).toBe(Number(e.velocity_max))
    expect(r.count).toBe(Number(e.count))
  })
  // Durations as the engine writes them: spelled out too.
  expect(await t.bucket({ series: 7, every: "60 minutes", agg: { velocity: ["avg", "max"], "*": ["count"] } })).toEqual(
    rows,
  )
  const bySeries = await t.bucket({ series: [1, 2], every: "1d", agg: { velocity: ["avg"] }, by: ["tags_id"] })
  expect(bySeries.map((r) => r.tags_id)).toEqual([1, 2])
})

test("bucket origin and grouping match the engine", async () => {
  const t = lake.table("iot.readings")
  const rows = await t.bucket({
    every: "6h",
    origin: "2026-01-01T02:00:00Z",
    agg: { velocity: ["min", "first", "last"] },
    by: ["tags_id"],
  })
  const expected = fx.sql(
    "SELECT date_bin(INTERVAL '6 hours', time, TIMESTAMP '2026-01-01 02:00:00') AS bucket, tags_id, min(velocity) AS velocity_min, first_value(velocity ORDER BY time) FILTER (WHERE velocity IS NOT NULL) AS velocity_first, last_value(velocity ORDER BY time) FILTER (WHERE velocity IS NOT NULL) AS velocity_last FROM iot.readings GROUP BY 1, 2 ORDER BY 1, 2",
  )
  expect(rows.length).toBe(5 * SERIES)
  const got = rows.map((r) => ({ ...r, bucket: r.bucket.toISOString().replace(".000Z", "Z") }))
  expect(got).toEqual(expected as typeof got)
})

test("unknown aggregates are errors", async () => {
  const agg = { velocity: ["median" as AggOp] }
  await expect(lake.table("iot.readings").bucket({ every: "1h", agg })).rejects.toThrow("unknown variant")
})

test("zod source generation", async () => {
  const src = await lake.table("iot.readings").zodSource()
  expect(src).toContain("export const Reading = z.object({")
  expect(src).toContain("time: z.coerce.date(),")
  const name = (table: string) => /export const (\w+)/.exec(zodSource({ name: table, schema: [] }))?.[1]
  expect(name("iot.devices")).toBe("Device")
  expect(name("iot.engine_readings")).toBe("EngineReading")
  expect(name("iot.status")).toBe("Status")
  expect(name("iot.address")).toBe("Address")
  expect(name("iot.analysis")).toBe("Analysis")
  expect(name("tags")).toBe("Tag")
})

test("tables() rejects an index that isn't a list of names", async () => {
  const index = new Lake("http://127.0.0.1:9/lake", { fetch: async () => Response.json({ tables: ["iot.readings"] }) })
  await expect(index.tables()).rejects.toThrow("not a list of table names")
})

test("tests cannot reach the network", async () => {
  await expect(fetch("https://example.com/")).rejects.toThrow("tests are offline")
})
