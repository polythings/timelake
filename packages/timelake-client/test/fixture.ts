// A small, deterministic warehouse written by the Rust engine (the `timelake` CLI) into a temp
// dir and served over HTTP with Range support, so tests compare the client against the engine
// without any network access.
import { spawnSync } from "node:child_process"
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import path from "node:path"

/** The CLI binary; build it with `cargo build -p timelake-cli` at the repo root. */
const CLI = process.env.TIMELAKE_CLI ?? path.resolve(import.meta.dir, "../../../target/debug/timelake")

// macOS checks a newly built executable on its first run: about 4 s for the debug CLI, which would
// count against the 5 s timeout of the first `beforeAll` that builds a fixture. Run it once here,
// when the test files import this module, where no timeout applies.
if (existsSync(CLI)) spawnSync(CLI, ["--version"])

export const SERIES = 12
export const START = Date.parse("2026-01-01T00:00:00Z")
export const STEP_MS = 5 * 60_000
export const POINTS = 288 // one day: one file per hour partition, so 24 files

export interface Fixture {
  /** Warehouse URL, as recorded in the table metadata. */
  url: string
  /** Local directory of the URL's path (`<dir>/lake` holds the warehouse). */
  dir: string
  /** Runs SQL through the engine and returns the rows. */
  sql(query: string): Record<string, unknown>[]
  /** Runs the CLI on the warehouse; returns its output. */
  cli(...args: string[]): string
  close(): void
}

/** Velocity of series `id` at point `i`: deterministic, with a null every 7th value. */
function velocity(id: number, i: number): number | null {
  return (i + id) % 7 === 0 ? null : ((id * 37 + i * 11) % 1000) / 10
}

function readingsCsv(): string {
  const lines = ["time,tags_id,velocity,name"]
  for (let i = 0; i < POINTS; i++) {
    const time = new Date(START + i * STEP_MS).toISOString()
    for (let id = 1; id <= SERIES; id++) lines.push(`${time},${id},${velocity(id, i) ?? ""},truck_${id}`)
  }
  return lines.join("\n") + "\n"
}

/** Serves `root` like a static host: whole files, or one byte range per request. */
function serve(root: string) {
  return Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const file = path.join(root, decodeURIComponent(new URL(req.url).pathname))
      if (!file.startsWith(root + path.sep)) return new Response(null, { status: 403 })
      const body = Bun.file(file)
      if (!(await body.exists())) return new Response(null, { status: 404 })
      const m = /^bytes=(\d+)-(\d+)$/.exec(req.headers.get("range") ?? "")
      if (!m) return new Response(body)
      const start = Number(m[1])
      const end = Math.min(Number(m[2]), body.size - 1)
      return new Response(body.slice(start, end + 1), {
        status: 206,
        headers: { "Content-Range": `bytes ${start}-${end}/${body.size}` },
      })
    },
  })
}

/**
 * Removes the last-value index from every snapshot, and the head object, like a commit by another
 * Iceberg writer.
 */
function dropLastValueIndex(tableDir: string) {
  const dir = path.join(tableDir, "metadata")
  rmSync(path.join(dir, "timelake-head"))
  for (const name of readdirSync(dir).filter((f) => f.endsWith(".metadata.json"))) {
    const meta = JSON.parse(readFileSync(path.join(dir, name), "utf8")) as {
      snapshots?: { summary?: Record<string, string> }[]
    }
    for (const s of meta.snapshots ?? []) delete s.summary?.["timelake.last-value-index"]
    writeFileSync(path.join(dir, name), JSON.stringify(meta))
  }
}

export function buildFixture(): Fixture {
  if (!existsSync(CLI)) throw new Error(`timelake CLI not found at ${CLI}: run \`cargo build -p timelake-cli\``)
  const root = mkdtempSync(path.join(tmpdir(), "timelake-client-"))
  const server = serve(root)
  const url = `http://127.0.0.1:${server.port}/lake`
  const cli = (...args: string[]) => {
    const r = spawnSync(CLI, ["-w", url, "-o", `timelake_local_root=${root}`, ...args], { encoding: "utf8" })
    if (r.status !== 0) throw new Error(`timelake ${args.join(" ")}: ${r.stderr}`)
    return r.stdout
  }

  const csv = path.join(root, "readings.csv")
  writeFileSync(csv, readingsCsv())
  const tags = path.join(root, "tags.csv")
  writeFileSync(tags, ["id,name", ...Array.from({ length: SERIES }, (_, i) => `${i + 1},truck_${i + 1}`)].join("\n"))
  const series = ["--time-column", "time", "--series-key", "tags_id", "--chunk", "hour"]
  // Small row groups, so files have several and row-group pruning and range coalescing matter.
  const props = ["--prop", "timelake.row-group-rows=32"]
  const schema = ["--schema", "time:timestamptz,tags_id:int,velocity:double,name:string"]
  cli("import", "iot.readings", csv, ...schema, ...series, ...props)
  cli("import", "iot.unindexed", csv, ...schema, ...series, ...props)
  cli("import", "iot.tags", tags)
  cli("index")
  dropLastValueIndex(path.join(root, "lake/iot/unindexed"))

  return {
    url,
    dir: root,
    cli,
    sql: (query) =>
      cli("sql", "--format", "json", query)
        .split("\n")
        .filter(Boolean)
        .map((l) => JSON.parse(l) as Record<string, unknown>),
    close: () => {
      void server.stop(true)
      rmSync(root, { recursive: true, force: true })
    },
  }
}
