// Appends with the WebAssembly writer and checks the table with the Rust engine (the CLI) and the
// client's reader. Needs the CLI: `cargo build -p timelake-cli` at the repo root.
import { afterAll, beforeAll, expect, test } from "bun:test"
import { mkdirSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs"
import path from "node:path"

import { Lake, TableWriter, memoryStore, type MaintainReport, type Store } from "../src/index"
import { buildFixture, type Fixture } from "./fixture"

let fx: Fixture

beforeAll(() => {
  fx = buildFixture()
})
afterAll(() => fx?.close())

/** A store over a local directory; `putIfAbsent` is atomic through an exclusive create. */
function dirStore(root: string): Store {
  const file = (key: string) => path.join(root, key)
  const walk = (dir: string): string[] =>
    readdirSync(dir, { withFileTypes: true }).flatMap((e) =>
      e.isDirectory() ? walk(path.join(dir, e.name)) : [path.join(dir, e.name)],
    )
  return {
    async get(key) {
      try {
        return new Uint8Array(readFileSync(file(key)))
      } catch {
        return null
      }
    },
    async getRange(key, start, length) {
      return new Uint8Array(readFileSync(file(key)).subarray(start, start + length))
    },
    async put(key, body) {
      mkdirSync(path.dirname(file(key)), { recursive: true })
      writeFileSync(file(key), body)
    },
    async putIfAbsent(key, body) {
      mkdirSync(path.dirname(file(key)), { recursive: true })
      try {
        writeFileSync(file(key), body, { flag: "wx" })
        return true
      } catch {
        return false
      }
    },
    async list(prefix) {
      const dir = path.dirname(file(prefix + "x"))
      let files: string[] = []
      try {
        files = walk(dir)
      } catch {
        return []
      }
      return files
        .map((f) => ({ key: path.relative(root, f).split(path.sep).join("/"), size: statSync(f).size }))
        .filter((o) => o.key.startsWith(prefix))
    },
    async delete(keys) {
      for (const k of keys) rmSync(file(k), { force: true })
    },
  }
}

const count = (table: string) => Number(fx.sql(`SELECT count(*) AS n FROM ${table}`)[0]?.n)

test("rows appended from JavaScript read back in the engine and the client", async () => {
  const schema = ["--schema", "time:timestamptz,device:int,temp:double"]
  fx.cli("create", "iot.live", ...schema, "--time-column", "time", "--series-key", "device")
  const writer = new TableWriter(dirStore(fx.dir), fx.url, "iot.live")
  const t0 = Date.parse("2026-01-01T00:00:00Z")
  const rows = Array.from({ length: 50 }, (_, i) => ({ time: new Date(t0 + i * 60_000), device: i % 5, temp: i }))
  const first = await writer.append(rows)
  expect(first).toMatchObject({ rows: 50, files: 1, attempts: 1, duplicate: false })

  // A newer write of one reading, epoch milliseconds and an ISO string as times, and a batch id.
  const second = await writer.append(
    [
      { time: t0, device: 0, temp: -1 },
      { time: "2026-01-02T00:00:00.000Z", device: 9, temp: 99 },
    ],
    { batchId: "b1" },
  )
  expect(second).toMatchObject({ rows: 2, files: 2, version: first.version + 1 })
  expect(await writer.append([{ time: t0, device: 1, temp: 0 }], { batchId: "b1" })).toMatchObject({
    duplicate: true,
  })

  expect(count("iot.live")).toBe(51)
  expect(fx.sql("SELECT temp FROM iot.live WHERE device = 0 AND time = TIMESTAMP '2026-01-01T00:00:00Z'")).toEqual([
    { temp: -1 },
  ])
  const latest = await new Lake(fx.url).table("iot.live").latest()
  expect(latest.map((r) => r.device).sort()).toEqual([0, 1, 2, 3, 4, 9])
  expect(fx.cli("info", "iot.live")).toContain("files")
})

test("bigints beyond 2^53 are written exactly", async () => {
  fx.cli("create", "iot.counters", "--schema", "time:timestamptz,id:long", "--time-column", "time")
  const writer = new TableWriter(dirStore(fx.dir), fx.url, "iot.counters")
  await writer.append([{ time: Date.parse("2026-01-01T00:00:00Z"), id: 2n ** 53n + 1n }])
  expect(fx.sql("SELECT CAST(id AS VARCHAR) AS id FROM iot.counters")).toEqual([{ id: "9007199254740993" }])

  // Without JSON.rawJSON (older runtimes), an unsafe bigint is an error, never a rounded number.
  const rawJSON = Object.getOwnPropertyDescriptor(JSON, "rawJSON")
  delete (JSON as { rawJSON?: unknown }).rawJSON
  try {
    await expect(writer.append([{ time: 0, id: -(2n ** 63n) }])).rejects.toThrow(/bigint/)
    expect((await writer.append([{ time: 0, id: 7n }])).rows).toBe(1)
  } finally {
    if (rawJSON) Object.defineProperty(JSON, "rawJSON", rawJSON)
  }
})

test("bad rows are rejected before anything is written", async () => {
  const writer = new TableWriter(dirStore(fx.dir), fx.url, "iot.readings")
  await expect(writer.append([{ time: Date.now(), tags_id: 1, nope: 1 }])).rejects.toThrow(/don't fit the table.*nope/)
  await expect(writer.append([{ tags_id: 1 }])).rejects.toThrow()
})

test("concurrent writers both commit", async () => {
  fx.cli("create", "iot.busy", "--schema", "time:timestamptz,device:int", "--time-column", "time")
  const a = new TableWriter(dirStore(fx.dir), fx.url, "iot.busy")
  const b = new TableWriter(dirStore(fx.dir), fx.url, "iot.busy")
  const rows = (d: number) => Array.from({ length: 10 }, (_, i) => ({ time: 1_767_225_600_000 + i, device: d }))
  const reports = await Promise.all([a.append(rows(1)), b.append(rows(2)), a.append(rows(3))])
  expect(reports.map((r) => r.version).sort()).toEqual([2, 3, 4])
  expect(count("iot.busy")).toBe(30)
})

test("the memory store holds a table copied into it", async () => {
  const store = memoryStore()
  const disk = dirStore(fx.dir)
  for (const o of await disk.list("lake/iot/live/")) await store.put(o.key, (await disk.get(o.key)) ?? new Uint8Array())
  const writer = new TableWriter(store, fx.url, "iot.live")
  expect((await writer.schema()).map((f) => f.name)).toEqual(["time", "device", "temp", "_ingest_ts"])
  const report = await writer.append([{ time: Date.parse("2026-01-03T00:00:00Z"), device: 1, temp: 3 }])
  expect(report.rows).toBe(1)
  expect(store.keys().some((k) => k.includes("time_day=2026-01-03"))).toBe(true)
})

test("trailing slashes on the warehouse are trimmed, in linear time", () => {
  expect(new TableWriter(memoryStore(), "s3://b/lake///", "iot.live").location).toBe("s3://b/lake/iot/live")
  const many = `s3://b${"/".repeat(100_000)}x`
  const started = performance.now()
  expect(new TableWriter(memoryStore(), many, "iot.live").location).toBe(`${many}/iot/live`)
  expect(new Lake(`${many}${"/".repeat(100_000)}`).url).toBe(many)
  expect(performance.now() - started).toBeLessThan(1000)
})

test("the writer and the reader take table names as the engine does", () => {
  expect(new TableWriter(memoryStore(), "s3://b/lake", "readings").location).toBe("s3://b/lake/default/readings")
  for (const bad of ["iot/x.live", "iot.a/b", "a.b.c", "iot."]) {
    expect(() => new TableWriter(memoryStore(), "s3://b/lake", bad)).toThrow("must be [namespace.]table")
    expect(() => new Lake("https://x/lake").table(bad)).toThrow("must be [namespace.]table")
  }
})

test("appends compact small files inline, and maintain compacts the rest", async () => {
  const schema = ["--schema", "time:timestamptz,device:int,temp:double"]
  const props = ["--prop", "timelake.compact-min-files=3", "--prop", "timelake.snapshot-retention=0s"]
  fx.cli("create", "iot.small", ...schema, "--time-column", "time", "--series-key", "device", ...props)
  const writer = new TableWriter(dirStore(fx.dir), fx.url, "iot.small")
  const t0 = Date.parse("2026-01-01T00:00:00Z")
  let rewritten = 0
  for (let i = 0; i < 4; i++) {
    const rows = Array.from({ length: 20 }, (_, j) => ({ time: t0 + (i * 20 + j) * 1000, device: j % 4, temp: i }))
    const report = await writer.append(rows)
    rewritten += report.maintenance?.filesRewritten ?? 0
  }
  expect(rewritten).toBe(3)
  const maintained = await writer.maintain()
  expect(maintained).toMatchObject({ filesRewritten: 2, filesWritten: 1, moreToCompact: false })
  // Every field of the interface, and no other: the type and the WebAssembly module agree.
  const fields = {
    partitionsCompacted: true,
    filesRewritten: true,
    filesWritten: true,
    rowsRewritten: true,
    bytesRewritten: true,
    duplicatesRemoved: true,
    moreToCompact: true,
    partitionsDropped: true,
    filesDropped: true,
    rollupsRefreshed: true,
    rollupRows: true,
    snapshotsExpired: true,
    objectsDeleted: true,
    seconds: true,
    version: true,
  } satisfies Record<keyof MaintainReport, true>
  expect(Object.keys(maintained).sort()).toEqual(Object.keys(fields).sort())
  expect(count("iot.small")).toBe(80)
  expect(fx.sql("SELECT count(*) AS n FROM iot.small WHERE temp = 3")).toEqual([{ n: 20 }])
})

test("the workers entry takes the module from initWasm", async () => {
  const workers = await import("../src/workers")
  const { wasmBase64 } = await import("../src/wasm/inline.js")
  workers.initWasm(Uint8Array.from(atob(wasmBase64), (c) => c.charCodeAt(0)))
  const writer = new workers.TableWriter(workers.memoryStore(), "s3://bucket/lake", "iot.none")
  await expect(writer.append([{ time: 0 }])).rejects.toThrow(/not found/)
})
