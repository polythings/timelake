/**
 * @timelake/client: read Timelake tables over HTTPS from the browser, Node, or serverless
 * functions. A WebAssembly build of Timelake's reader parses Iceberg metadata, prunes files and
 * row groups, and decodes Parquet; this package fetches only the byte ranges it asks for.
 *
 * ```ts
 * import { z } from "zod"
 * import { Lake } from "@timelake/client"
 *
 * const Reading = z.object({ time: z.coerce.date(), tags_id: z.number().int(), velocity: z.number().nullable() })
 * const lake = new Lake("https://<store>.public.blob.vercel-storage.com/demo")
 * const readings = lake.table("tsbs.readings", Reading)
 * await readings.latest({ series: [1, 2, 3] })                  // newest row per truck
 * await readings.range({ series: 7, from: "2026-01-03T12:00:00Z", to: "2026-01-03T13:00:00Z" })
 * await readings.bucket({ series: 7, every: "1h", agg: { velocity: ["avg", "max"] } })
 * ```
 */

import { z } from "zod"

import { ensureWasm } from "./init.ts"
import { durationMicros, parseTableName, tablesIndex, timeMicros, trimTrailingSlashes } from "./util.ts"
import type { DurationInput, TimeInput } from "./util.ts"
import {
  ParquetFile,
  Query,
  Table as WasmTable,
  footerPrefetchBytes,
  headUri,
  heapPeakBytes,
  hintUri,
  memoryBytes,
  metadataUri,
  tableLocation,
  version as wasmVersion,
} from "./wasm/timelake_wasm.js"

export { initWasm } from "./init.ts"
export type { DurationInput, TimeInput } from "./util.ts"
export { memoryStore, r2Store, TableWriter } from "./writer.ts"
export type {
  AppendReport,
  AppendRowsOptions,
  MaintainOptions,
  MaintainReport,
  R2BucketLike,
  Store,
  StoredObject,
  Upload,
} from "./writer.ts"

/** Bytes of memory the WebAssembly module has grown to: the peak of what reads and writes used so far. */
export function wasmMemoryBytes(): number {
  ensureWasm()
  return memoryBytes()
}

/** The most heap bytes the WebAssembly module had in use at once so far (below {@link wasmMemoryBytes} by what the allocator keeps). */
export function wasmHeapPeakBytes(): number {
  ensureWasm()
  return heapPeakBytes()
}

/** Version of the embedded WASM reader. */
export function readerVersion(): string {
  ensureWasm()
  return wasmVersion()
}

// --- types -----------------------------------------------------------------------------------------

export type Row = Record<string, unknown>
export type Scalar = string | number
/** Series filter: one value or a list for the (first) series key, or `{ column: value(s) }`. */
export type SeriesFilter = Scalar | Scalar[] | Record<string, Scalar | Scalar[]>
export type AggOp = "count" | "sum" | "avg" | "min" | "max" | "first" | "last"
export type AggMap = { readonly [column: string]: readonly AggOp[] }

/** Output column names of an aggregation map: `velocity_avg`, …; `"*": ["count"]` is `count`. */
export type AggKeys<A extends AggMap> = {
  [C in keyof A & string]: C extends "*" ? "count" : `${C}_${A[C][number]}`
}[keyof A & string]

export type BucketRow<A extends AggMap, By extends string = never> = { bucket: Date } & {
  [K in AggKeys<A>]: number | null
} & {
  [K in By]: Scalar | null
}

/** The part of `fetch` the client uses. */
export type Fetch = (url: string, init?: RequestInit) => Promise<Response>

export interface LakeOptions {
  /** fetch implementation (defaults to the global fetch). */
  fetch?: Fetch
  /** Maximum concurrent HTTP requests per query. Default 16. */
  concurrency?: number
  /** How long a loaded table version is reused, in ms. Default 5000. */
  cacheMs?: number
  /** Rewrite object URLs before fetching (e.g. when a warehouse was copied to a new host). */
  rewriteUrl?: (url: string) => string
}

export interface SnapshotInfo {
  id: string
  timestampMs: number
  operation: string
  summary: Record<string, string>
}

export interface TableInfo {
  name: string
  location: string
  formatVersion: number
  schema: { id: number; name: string; type: string; required: boolean }[]
  timeColumn: string | null
  seriesKey: string[]
  chunk: string | null
  dedup: boolean
  properties: Record<string, string>
  currentSnapshot: SnapshotInfo | null
  lastValueIndex: string | null
  snapshotCount: number
  /** Newest first, at most 50. */
  snapshots: SnapshotInfo[]
  lastUpdatedMs: number
  /** Committed version (`vN.metadata.json`). */
  version: number
  /** Number of manifests in the current snapshot; `null` when the table was read from its head. */
  manifests: number | null
  /** Live data files; `partition` is the time partition (hours, days or months since the epoch). */
  files: { path: string; size: number; records: number; partition: number | null }[]
}

export interface RangeOptions {
  series?: SeriesFilter
  from?: TimeInput
  to?: TimeInput
  columns?: string[]
  limit?: number
  order?: "asc" | "desc"
}

export interface BucketOptions<A extends AggMap, By extends string> {
  series?: SeriesFilter
  from?: TimeInput
  to?: TimeInput
  every: DurationInput
  /** A bucket boundary; buckets are aligned to it (default: the Unix epoch). */
  origin?: TimeInput
  agg: A
  /** Also group by these columns (e.g. the series key) instead of merging series. */
  by?: readonly By[]
}

/** Request statistics of the last query on a table handle. */
export interface QueryStats {
  requests: number
  bytes: number
  files: number
  ms: number
}

// --- helpers ---------------------------------------------------------------------------------------

const DEFAULT_CONCURRENCY = 16
const DEFAULT_CACHE_MS = 5000

const optionalMicros = (t: TimeInput | undefined) => (t === undefined ? undefined : timeMicros(t))

const emptyStats = (): QueryStats => ({ requests: 0, bytes: 0, files: 0, ms: 0 })

/** Runs at most `max` tasks at once, the rest in FIFO order. */
class Limiter {
  private active = 0
  private queue: (() => void)[] = []
  constructor(private max: number) {}
  async run<T>(f: () => Promise<T>): Promise<T> {
    if (this.active >= this.max) await new Promise<void>((r) => this.queue.push(r))
    this.active++
    try {
      return await f()
    } finally {
      this.active--
      this.queue.shift()?.()
    }
  }
}

/**
 * The requests of one call, loading the table included: at most `concurrency` in flight, counted
 * for `lastStats`. The limit applies to single requests only: a task waiting for its own requests
 * must never hold a slot they need.
 */
class Requests {
  readonly stats = emptyStats()
  private limiter: Limiter

  constructor(private lake: Lake) {
    this.limiter = new Limiter(lake.options.concurrency)
  }

  /** The response to a GET of `url`, or `undefined` if it is absent (HTTP 403 or 404). Read its body with {@link read}. */
  open(url: string): Promise<Response | undefined> {
    return this.limiter.run(() => {
      this.stats.requests++
      return this.lake.get(url, undefined, true)
    })
  }

  /** Whether `url` exists (a HEAD request; HTTP 403 and 404 mean it doesn't). */
  exists(url: string): Promise<boolean> {
    return this.limiter.run(async () => {
      this.stats.requests++
      return (await this.lake.get(url, { method: "HEAD" }, true)) !== undefined
    })
  }

  /** The body of a response from {@link open}. */
  async read(r: Response): Promise<Uint8Array> {
    const b = new Uint8Array(await r.arrayBuffer())
    this.stats.bytes += b.length
    return b
  }

  /** The whole object, or bytes `[start, end)` of it. */
  bytes(url: string, range?: [number, number]): Promise<Uint8Array> {
    return this.limiter.run(async () => {
      const init = range ? { headers: { Range: `bytes=${range[0]}-${range[1] - 1}` } } : undefined
      this.stats.requests++
      const r = await this.lake.get(url, init)
      let b = new Uint8Array(await r.arrayBuffer())
      this.stats.bytes += b.length
      // Servers that ignore Range answer 200 with the whole object.
      if (range && r.status === 200 && b.length > range[1] - range[0]) b = b.slice(range[0], range[1])
      return b
    })
  }
}

/** Runs a query over one Parquet file held in memory (the last-value index). */
function queryWholeFile(bytes: Uint8Array, spec: object): unknown[] {
  const query = new Query(spec)
  try {
    const file = new ParquetFile(bytes.length, bytes)
    try {
      file.plan(spec)
      file.addRange(0, bytes)
      query.push(file)
    } finally {
      file.free()
    }
    return JSON.parse(query.finish()) as unknown[]
  } finally {
    query.free()
  }
}

const TableNames = z.array(z.string())

// --- Lake ------------------------------------------------------------------------------------------

/** A Timelake warehouse served over HTTP(S): Vercel Blob, S3/R2/GCS public URLs, a CDN, … */
export class Lake {
  readonly url: string
  readonly options: Required<Omit<LakeOptions, "rewriteUrl">> & Pick<LakeOptions, "rewriteUrl">

  constructor(url: string, options: LakeOptions = {}) {
    this.url = trimTrailingSlashes(url)
    this.options = {
      fetch: options.fetch ?? ((target, init) => globalThis.fetch(target, init)),
      concurrency: options.concurrency ?? DEFAULT_CONCURRENCY,
      cacheMs: options.cacheMs ?? DEFAULT_CACHE_MS,
      rewriteUrl: options.rewriteUrl,
    }
  }

  /** Table names (`namespace.table`) from the warehouse's `timelake-tables.json`. */
  async tables(): Promise<string[]> {
    const url = tablesIndex(this.url)
    const parsed = TableNames.safeParse(await (await this.get(url)).json())
    if (!parsed.success) throw new Error(`${url}: not a list of table names (${parsed.error.message})`)
    return parsed.data
  }

  /**
   * A table handle. Pass a Zod schema to validate and type rows (use `z.coerce.date()` for time
   * columns; values arrive as ISO strings). Without one, rows are `Record<string, unknown>`.
   */
  table<S extends z.ZodType = z.ZodType<Row>>(name: string, schema?: S): TableHandle<z.output<S>> {
    return new TableHandle(this, name, schema)
  }

  /**
   * GETs `url` (after `rewriteUrl`). Throws on HTTP errors, except that a 403 or 404 returns
   * `undefined` when `optional` is set: public S3 and CloudFront buckets without `s3:ListBucket`
   * answer 403 for a missing key.
   * @internal
   */
  async get(url: string, init?: RequestInit): Promise<Response>
  async get(url: string, init: RequestInit | undefined, optional: true): Promise<Response | undefined>
  async get(url: string, init?: RequestInit, optional = false): Promise<Response | undefined> {
    const target = this.options.rewriteUrl?.(url) ?? url
    const r = await this.options.fetch(target, init)
    if (optional && (r.status === 404 || r.status === 403)) {
      void r.body?.cancel()
      return undefined
    }
    if (!r.ok) throw new Error(`GET ${target}: HTTP ${r.status}`)
    return r
  }
}

type Loaded = {
  wasm: WasmTable
  info: Omit<TableInfo, "files" | "name" | "version" | "manifests">
  files: TableInfo["files"]
  version: number
  manifests: number | null
  /** Resolves to true if a newer version than `version` existed when it was loaded. */
  stale: Promise<boolean>
}

/** One table of a {@link Lake}; rows are typed `T` (validated when a Zod schema was given). */
export class TableHandle<T = Row> {
  private loaded: Promise<Loaded> | null = null
  private loadedAt = 0
  #location: string | undefined
  /** Statistics of the last query, the requests that loaded the table version included. */
  lastStats: QueryStats = emptyStats()

  constructor(
    readonly lake: Lake,
    readonly name: string,
    private schema?: z.ZodType,
  ) {
    // Checked now, before the WebAssembly module may be loaded, so a bad name fails here.
    parseTableName(name)
  }

  /** The table's root URI, as the module builds it. */
  private get location(): string {
    ensureWasm()
    this.#location ??= tableLocation(this.lake.url, this.name)
    return this.#location
  }

  private metadataUrl(version: number): string {
    return metadataUri(this.location, version)
  }

  /**
   * Loads (or reuses) the current table version. A version loaded earlier is reused within
   * `cacheMs`, and after that while no newer one exists (checked as the next query runs).
   */
  private load(requests: Requests): Promise<Loaded> {
    if (this.loaded && Date.now() - this.loadedAt < this.lake.options.cacheMs) return this.loaded
    const previous = this.loaded
    return this.keep(
      previous
        ? previous.then((l) => ({ ...l, stale: this.outdated(l.version, requests) }))
        : this.fetchVersion(requests),
    )
  }

  /**
   * Whether a version after `version` exists. Maintenance deletes the oldest metadata files, so the
   * next version's may be gone too when this one's is: then newer versions exist.
   */
  private async outdated(version: number, requests: Requests): Promise<boolean> {
    const [next, self] = await Promise.all([
      requests.exists(this.metadataUrl(version + 1)),
      requests.exists(this.metadataUrl(version)),
    ])
    return next || !self
  }

  /** Loads version `version` or the newest after it, through the metadata chain. */
  private reload(version: number, requests: Requests): Promise<Loaded> {
    return this.keep(this.newestFrom(version, requests))
  }

  /** Makes `loaded` the version later calls reuse. */
  private keep(loaded: Promise<Loaded>): Promise<Loaded> {
    this.loadedAt = Date.now()
    loaded.catch(() => {
      if (this.loaded === loaded) this.loaded = null
    })
    this.loaded = loaded
    return loaded
  }

  /**
   * Loads the current version: from the table's head object (`metadata/timelake-head`: metadata,
   * files, and when they fit the last-value index and file footers) when it has one, checked for
   * a newer version while the first query runs; otherwise through the version hint, metadata,
   * manifest list and manifests.
   */
  private async fetchVersion(requests: Requests): Promise<Loaded> {
    ensureWasm()
    const [head, hintResponse] = await Promise.all([
      requests.open(headUri(this.location)),
      requests.open(hintUri(this.location)),
    ])
    const hint = hintResponse ? new TextDecoder().decode(await requests.read(hintResponse)).trim() : undefined
    const hinted = hint === undefined ? undefined : Number(hint)
    if (hinted !== undefined && !(Number.isSafeInteger(hinted) && hinted >= 0)) {
      void head?.body?.cancel()
      throw new Error(`${this.name}: bad version hint ${JSON.stringify(hint)}`)
    }
    if (head) {
      const loaded = this.fromHead(await requests.read(head), hinted, requests)
      if (loaded) return loaded
    }
    if (hinted === undefined) {
      throw new Error(`${this.name}: table not found (no head or version hint; HTTP 403 or 404 for both)`)
    }
    const loaded = await this.fromMetadata(hinted, requests)
    if (!loaded) throw new Error(`${this.name}: metadata v${hinted} not found`)
    return loaded
  }

  /**
   * Version `version` or the newest after it. When the metadata of `version` is gone (expired by
   * maintenance while this handle was many versions behind), starts again from the head and hint.
   */
  private async newestFrom(version: number, requests: Requests): Promise<Loaded> {
    const loaded = await this.fromMetadata(version, requests)
    if (loaded) return loaded
    const fresh = await this.fetchVersion(requests)
    if (!(await fresh.stale)) return fresh
    return (await this.fromMetadata(fresh.version + 1, requests)) ?? fresh
  }

  /**
   * The newest version from `known` (which exists) on. The hint and the head are written after
   * each commit, so they may lag; a handle loaded earlier may be many commits behind. Versions are
   * consecutive, so HEAD requests double their step from `known` until a version is absent, then
   * halve it: a handle n versions behind needs about 2 log2(n) requests.
   */
  private async newestVersion(known: number, requests: Requests): Promise<number> {
    let found = known
    let step = 1
    while (await requests.exists(this.metadataUrl(found + step))) {
      found += step
      step *= 2
    }
    let absent = found + step
    while (absent - found > 1) {
      const mid = found + Math.floor((absent - found) / 2)
      if (await requests.exists(this.metadataUrl(mid))) found = mid
      else absent = mid
    }
    return found
  }

  /**
   * Version `version`, or the newest after it, through the metadata, manifest list and manifests;
   * `undefined` if neither exists. Looks for the next version while fetching this one.
   */
  private async fromMetadata(version: number, requests: Requests): Promise<Loaded | undefined> {
    const [current, next] = await Promise.all([
      requests.open(this.metadataUrl(version)),
      requests.exists(this.metadataUrl(version + 1)),
    ])
    let response = current
    if (next) {
      void current?.body?.cancel()
      version = await this.newestVersion(version + 1, requests)
      response = await requests.open(this.metadataUrl(version))
    }
    if (!response) return undefined
    const wasm = new WasmTable(new TextDecoder().decode(await requests.read(response)))
    let manifests: string[] = []
    try {
      if (wasm.manifestList) {
        manifests = wasm.addManifestList(await requests.bytes(wasm.manifestList))
        for (const bytes of await Promise.all(manifests.map((path) => requests.bytes(path)))) wasm.addManifest(bytes)
      }
    } catch (e) {
      wasm.free()
      throw e
    }
    return {
      wasm,
      info: wasm.info() as Loaded["info"],
      files: wasm.files() as TableInfo["files"],
      version,
      manifests: manifests.length,
      stale: Promise.resolve(false),
    }
  }

  /** The version a head describes; `undefined` if it can't be read here or the hint is newer. */
  private fromHead(bytes: Uint8Array, hinted: number | undefined, requests: Requests): Loaded | undefined {
    let wasm: WasmTable
    try {
      wasm = WasmTable.fromHead(bytes)
    } catch {
      return undefined // a format or layout this reader doesn't know
    }
    const version = wasm.version
    if (version === undefined || (hinted !== undefined && hinted > version)) {
      wasm.free()
      return undefined
    }
    return {
      wasm,
      info: wasm.info() as Loaded["info"],
      files: wasm.files() as TableInfo["files"],
      version,
      manifests: null,
      stale: requests.exists(this.metadataUrl(version + 1)),
    }
  }

  /** Drops the cached table version; the next call loads the latest commit. */
  refresh(): this {
    this.loaded = null
    return this
  }

  /** Contract, schema, snapshot and live files of the current version. */
  async info(): Promise<TableInfo> {
    const requests = new Requests(this.lake)
    let l = await this.load(requests)
    if (await l.stale) l = await this.reload(l.version + 1, requests)
    return { name: this.name, ...l.info, version: l.version, manifests: l.manifests, files: l.files }
  }

  /**
   * Runs `query` on the loaded version while the check for a newer version is in flight. If the
   * version turns out outdated, its result is dropped and the query runs on the newest version.
   * The requests of both, and of loading the table, become `lastStats` when it succeeds.
   */
  private async run<R>(query: (l: Loaded, requests: Requests) => Promise<R>): Promise<R> {
    const t0 = performance.now()
    const requests = new Requests(this.lake)
    const l = await this.load(requests)
    const [result, stale] = await Promise.allSettled([query(l, requests), l.stale])
    if (stale.status === "rejected") throw stale.reason
    let value: R
    if (!stale.value) {
      if (result.status === "rejected") throw result.reason
      value = result.value
    } else {
      value = await query(await this.reload(l.version + 1, requests), requests)
    }
    this.lastStats = { ...requests.stats, ms: performance.now() - t0 }
    return value
  }

  private seriesSpec(info: Loaded["info"], series?: SeriesFilter): Record<string, Scalar[]> {
    if (series === undefined) return {}
    if (typeof series === "object" && !Array.isArray(series)) {
      return Object.fromEntries(Object.entries(series).map(([k, v]) => [k, Array.isArray(v) ? v : [v]]))
    }
    const key = info.seriesKey[0]
    if (!key) throw new Error(`${this.name} has no series key; pass { column: value } instead`)
    return { [key]: Array.isArray(series) ? series : [series] }
  }

  /** Runs a query spec over the files that may match it. */
  private async scan(wasm: WasmTable, spec: object, requests: Requests): Promise<unknown[]> {
    const files = wasm.planFiles(spec) as TableInfo["files"]
    const prefetch = footerPrefetchBytes()
    const query = new Query(spec)
    // Every file opened so far, until it is pushed to the query; freed whatever fails.
    const opened: (ParquetFile | undefined)[] = []
    try {
      // Fetch files concurrently, decode in order (decoding is synchronous WASM work). Wait for
      // all fetches to settle before freeing anything, so none adds bytes to a freed file.
      const fetched = await Promise.allSettled(
        files.map(async ({ path, size }, i) => {
          let file = wasm.openFile(path, size)
          if (!file) {
            let tail = await requests.bytes(path, [Math.max(0, size - prefetch), size])
            const footer = ParquetFile.footerLength(tail)
            if (footer > tail.length) tail = await requests.bytes(path, [size - footer, size])
            file = new ParquetFile(size, tail)
          }
          opened[i] = file
          const ranges = file.plan(spec) as [number, number][]
          const chunks = await Promise.all(ranges.map((r) => requests.bytes(path, r)))
          ranges.forEach(([start], j) => file.addRange(start, chunks[j] as Uint8Array))
        }),
      )
      for (const f of fetched) if (f.status === "rejected") throw f.reason
      for (const [i, file] of opened.entries()) {
        if (!file) continue
        opened[i] = undefined
        try {
          query.push(file)
        } finally {
          file.free()
        }
      }
      requests.stats.files = files.length
      return JSON.parse(query.finish()) as unknown[]
    } finally {
      for (const file of opened) file?.free()
      query.free()
    }
  }

  private validate(rows: unknown[], columns?: string[]): T[] {
    if (!this.schema) return rows as T[]
    let schema = this.schema
    // A projection returns some columns only: validate against the matching part of the schema.
    const pick = (schema as unknown as { pick?: (m: Record<string, true>) => z.ZodType }).pick
    if (columns?.length && typeof pick === "function") {
      schema = pick.call(schema, Object.fromEntries(columns.map((c) => [c, true as const])))
    }
    return rows.map((r) => schema.parse(r) as T)
  }

  /**
   * The newest row of each series, from the table's last-value index (one small file). Tables
   * without the index (e.g. written by another Iceberg writer) are scanned instead.
   */
  async latest<C extends keyof T & string>(opts: { series?: SeriesFilter; columns: C[] }): Promise<Pick<T, C>[]>
  async latest(opts?: { series?: SeriesFilter; columns?: string[] }): Promise<T[]>
  async latest(opts: { series?: SeriesFilter; columns?: string[] } = {}): Promise<T[]> {
    const rows = await this.run(async ({ wasm, info }, requests) => {
      if (!info.timeColumn) throw new Error(`${this.name} has no time column, so no latest rows`)
      const spec = {
        timeColumn: info.timeColumn,
        series: this.seriesSpec(info, opts.series),
        columns: opts.columns ?? [],
      }
      const index = info.lastValueIndex
      if (!index) return this.scan(wasm, { ...spec, latestBy: info.seriesKey }, requests)
      requests.stats.files = 1
      // The index came with the head, or it is one small file.
      const inline = wasm.queryLastValues(spec)
      if (inline !== undefined) return JSON.parse(inline) as unknown[]
      return queryWholeFile(await requests.bytes(index), spec)
    })
    return this.validate(rows, opts.columns)
  }

  /** Rows of the selected series in `[from, to)`, ordered by time. */
  async range<C extends keyof T & string>(opts: RangeOptions & { columns: C[] }): Promise<Pick<T, C>[]>
  async range(opts?: RangeOptions): Promise<T[]>
  async range(opts: RangeOptions = {}): Promise<T[]> {
    const rows = await this.run(({ wasm, info }, requests) => {
      const spec = {
        timeColumn: info.timeColumn,
        series: this.seriesSpec(info, opts.series),
        from: optionalMicros(opts.from),
        to: optionalMicros(opts.to),
        columns: opts.columns ?? [],
        limit: opts.limit,
        descending: opts.order === "desc",
      }
      return this.scan(wasm, spec, requests)
    })
    return this.validate(rows, opts.columns)
  }

  /** Time-bucketed aggregates, e.g. `{ every: "1h", agg: { velocity: ["avg", "max"] } }`. */
  async bucket<A extends AggMap, By extends string = never>(opts: BucketOptions<A, By>): Promise<BucketRow<A, By>[]> {
    const aggs = Object.entries(opts.agg).flatMap(([column, ops]) => ops.map((op) => ({ column, op })))
    const rows = (await this.run(({ wasm, info }, requests) => {
      if (!info.timeColumn) throw new Error(`${this.name} has no time column`)
      const spec = {
        timeColumn: info.timeColumn,
        series: this.seriesSpec(info, opts.series),
        from: optionalMicros(opts.from),
        to: optionalMicros(opts.to),
        bucket: {
          every: durationMicros(opts.every),
          origin: optionalMicros(opts.origin),
          by: [...(opts.by ?? [])],
          aggs,
        },
      }
      return this.scan(wasm, spec, requests)
    })) as (Row & { bucket: string })[]
    return rows.map((r) => ({ ...r, bucket: new Date(r.bucket) })) as unknown as BucketRow<A, By>[]
  }

  /** TypeScript source of a Zod schema for this table's rows (paste it into your code). */
  async zodSource(exportName?: string): Promise<string> {
    return zodSource(await this.info(), exportName)
  }
}

// --- Zod helpers -------------------------------------------------------------------------------------

function zodType(icebergType: string): string {
  if (icebergType.startsWith("timestamp")) return "z.coerce.date()"
  switch (icebergType) {
    case "int":
    case "long":
      return "z.number().int()"
    case "float":
    case "double":
      return "z.number()"
    case "boolean":
      return "z.boolean()"
    default:
      return "z.string()"
  }
}

/** `word` without a trailing `s` unless it follows s, i or u: `Readings` → `Reading`; `Status`, `Address`, `Analysis` stay. */
function singular(word: string): string {
  return /[^siu]s$/i.test(word) ? word.slice(0, -1) : word
}

/**
 * Zod schema source for a table's rows (time columns coerce ISO strings to `Date`). The schema is
 * named after the table in PascalCase, without a trailing `s` unless it follows s, i or u
 * (`readings` → `Reading`, `status` → `Status`), unless `exportName` is given.
 */
export function zodSource(info: Pick<TableInfo, "name" | "schema">, exportName?: string): string {
  const table = info.name.split(".").at(-1) ?? info.name
  const name = exportName ?? singular(table.replace(/(^|[_-])(\w)/g, (_, __, c: string) => c.toUpperCase()))
  const fields = info.schema.map(
    (f) =>
      `  ${/^[A-Za-z_]\w*$/.test(f.name) ? f.name : JSON.stringify(f.name)}: ${zodType(f.type)}${f.required ? "" : ".nullable()"},`,
  )
  return `export const ${name} = z.object({\n${fields.join("\n")}\n})\nexport type ${name} = z.infer<typeof ${name}>\n`
}
