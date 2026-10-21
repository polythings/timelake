/**
 * Writing Timelake tables from JavaScript: Cloudflare Workers, Node, Deno, Bun, or a browser. The
 * WebAssembly module prepares rows, writes Parquet, and commits exactly as the Rust engine does;
 * this module gives it a store to read and write objects in.
 *
 * ```ts
 * import { TableWriter, r2Store } from "@timelake/client"
 *
 * // In a Worker with an R2 binding LAKE on the bucket that holds s3://my-bucket/lake:
 * const writer = new TableWriter(r2Store(env.LAKE), "s3://my-bucket/lake", "iot.readings")
 * await writer.append([{ time: new Date(), device: 7, temp: 21.5 }])
 * ```
 */

import { ensureWasm } from "./init.ts"
import { trimTrailingSlashes } from "./util.ts"
import { Writer as WasmWriter, tableLocation } from "./wasm/timelake_wasm.js"

/** An object in a store, as {@link Store.list} returns it. */
export interface StoredObject {
  key: string
  size: number
}

/** A multipart upload: parts in order (each but the last at least 5 MiB on S3 and R2), then complete. */
export interface Upload {
  putPart(part: Uint8Array): Promise<void>
  complete(): Promise<void>
  abort(): Promise<void>
}

/**
 * Where a writer keeps a warehouse's objects, by key (the path after the bucket). `putIfAbsent`
 * must be atomic: two writers putting the same new key must not both succeed. Commits rely on it.
 */
export interface Store {
  get(key: string): Promise<Uint8Array | null>
  getRange(key: string, start: number, length: number): Promise<Uint8Array>
  put(key: string, body: Uint8Array): Promise<void>
  /** Writes only if no object has the key; false if one had. */
  putIfAbsent(key: string, body: Uint8Array): Promise<boolean>
  list(prefix: string): Promise<StoredObject[]>
  delete(keys: string[]): Promise<void>
  exists?(key: string): Promise<boolean>
  /** Multipart uploads, for files larger than a part; without it, files are written in one put. */
  upload?(key: string): Promise<Upload>
}

export interface AppendRowsOptions {
  /** Idempotency key: appending the same batch id twice commits once. */
  batchId?: string
  /** Extra snapshot summary properties. */
  properties?: Record<string, string>
  /** Run maintenance after the append (default: the table's `auto-maintain`, on unless turned off). */
  maintain?: boolean
  /** Bytes of files inline compaction may read after the append. Default 64 MiB, which keeps a Worker request well inside its CPU limit. */
  maxCompactBytes?: number
}

export interface MaintainOptions {
  /** Merge small files into sorted files of the target size (default true). */
  compact?: boolean
  /** Drop partitions past the table's retention (default true). */
  retention?: boolean
  /** Expire old snapshots and delete files only they used (default true). */
  expire?: boolean
  /** Stop compacting once this many bytes of files were read (at least one group is compacted); the next run continues. */
  maxBytes?: number
}

export interface MaintainReport {
  partitionsCompacted: number
  filesRewritten: number
  filesWritten: number
  rowsRewritten: number
  bytesRewritten: number
  duplicatesRemoved: number
  /** Compaction stopped at `maxBytes` with more to do. */
  moreToCompact: boolean
  partitionsDropped: number
  filesDropped: number
  snapshotsExpired: number
  objectsDeleted: number
  /** The table version after maintenance. */
  version: number
}

export interface AppendReport {
  /** Rows committed, after removing rows with the same (series, time) within the append. */
  rows: number
  /** Data files and their bytes. */
  files: number
  bytes: number
  /** The table version after the commit. */
  version: number
  /** The snapshot, as a string (it doesn't fit a JavaScript number). */
  snapshotId: string
  /** Commit attempts: more than one means other writers committed at the same time. */
  attempts: number
  /** The batch id was already committed; nothing was written. */
  duplicate: boolean
  /** Inline maintenance after the append, if it did something. */
  maintenance?: MaintainReport
  /** Why inline maintenance failed; the append is committed either way. Don't retry the append. */
  maintenanceError?: string
}

/** The part of a Cloudflare R2 binding (`R2Bucket`) the adapter uses. */
export interface R2BucketLike {
  get(
    key: string,
    options?: { range?: { offset: number; length: number } },
  ): Promise<{ arrayBuffer(): Promise<ArrayBuffer> } | null>
  head(key: string): Promise<object | null>
  put(key: string, value: Uint8Array, options?: { onlyIf?: Headers }): Promise<object | null>
  list(options: { prefix: string; cursor?: string }): Promise<{
    objects: { key: string; size: number }[]
    truncated: boolean
    cursor?: string
  }>
  delete(keys: string[]): Promise<void>
  createMultipartUpload(key: string): Promise<{
    uploadPart(partNumber: number, value: Uint8Array): Promise<{ partNumber: number; etag: string }>
    complete(parts: { partNumber: number; etag: string }[]): Promise<object>
    abort(): Promise<void>
  }>
}

/** A {@link Store} over a Cloudflare R2 binding. */
export function r2Store(bucket: R2BucketLike): Store {
  return {
    async get(key) {
      const o = await bucket.get(key)
      return o ? new Uint8Array(await o.arrayBuffer()) : null
    },
    async getRange(key, start, length) {
      const o = await bucket.get(key, { range: { offset: start, length } })
      if (!o) throw new Error(`not found: ${key}`)
      return new Uint8Array(await o.arrayBuffer())
    },
    async put(key, body) {
      await bucket.put(key, body)
    },
    async putIfAbsent(key, body) {
      // R2 returns null when the precondition fails: an object already has the key.
      return (await bucket.put(key, body, { onlyIf: new Headers({ "If-None-Match": "*" }) })) !== null
    },
    async list(prefix) {
      const out: StoredObject[] = []
      let cursor: string | undefined
      do {
        const page = await bucket.list({ prefix, cursor })
        out.push(...page.objects.map((o) => ({ key: o.key, size: o.size })))
        cursor = page.truncated ? page.cursor : undefined
      } while (cursor)
      return out
    },
    async delete(keys) {
      // R2 deletes at most 1,000 keys per call.
      for (let i = 0; i < keys.length; i += 1000) await bucket.delete(keys.slice(i, i + 1000))
    },
    async exists(key) {
      return (await bucket.head(key)) !== null
    },
    async upload(key) {
      const upload = await bucket.createMultipartUpload(key)
      const parts: { partNumber: number; etag: string }[] = []
      return {
        async putPart(part) {
          parts.push(await upload.uploadPart(parts.length + 1, part))
        },
        async complete() {
          await upload.complete(parts)
        },
        abort: () => upload.abort(),
      }
    },
  }
}

/** A {@link Store} in memory, for tests and examples. */
export function memoryStore(): Store & { keys(): string[] } {
  const objects = new Map<string, Uint8Array>()
  return {
    keys: () => [...objects.keys()].sort(),
    async get(key) {
      return objects.get(key) ?? null
    },
    async getRange(key, start, length) {
      const o = objects.get(key)
      if (!o) throw new Error(`not found: ${key}`)
      return o.slice(start, start + length)
    },
    async put(key, body) {
      objects.set(key, body.slice())
    },
    async putIfAbsent(key, body) {
      if (objects.has(key)) return false
      objects.set(key, body.slice())
      return true
    },
    async list(prefix) {
      return [...objects.entries()]
        .filter(([k]) => k.startsWith(prefix))
        .map(([key, body]) => ({ key, size: body.length }))
    },
    async delete(keys) {
      for (const k of keys) objects.delete(k)
    },
  }
}

/** `scheme://authority` of a warehouse URI, which store keys leave out. */
function rootOf(warehouse: string): string {
  const m = /^([a-z][a-z0-9+.-]*:\/\/[^/]*)/i.exec(warehouse)
  if (!m?.[1]) throw new Error(`warehouse must be a URI like s3://bucket/prefix, got ${warehouse}`)
  return m[1]
}

/**
 * Rows as newline-delimited JSON: Dates as ISO strings (as JSON does), bigints as exact number
 * literals. `JSON.rawJSON` (ES2025) writes the digits; without it, a bigint beyond 2^53 is an error.
 */
function rowsJson(rows: readonly Record<string, unknown>[]): string {
  const { rawJSON } = JSON as { rawJSON?: (text: string) => unknown }
  const replacer = (key: string, v: unknown) => {
    if (typeof v !== "bigint") return v
    if (rawJSON) return rawJSON(v.toString())
    const n = Number(v)
    if (!Number.isSafeInteger(n)) throw new RangeError(`${key}: bigint ${v} needs JSON.rawJSON to be written exactly`)
    return n
  }
  return rows.map((r) => JSON.stringify(r, replacer)).join("\n")
}

/**
 * Appends to one table. The table must exist (create it with the CLI or Python). The writer keeps
 * the version it last committed, so a steady writer doesn't reload the table before each append;
 * if another writer committed in between, the commit notices and retries on top of it.
 *
 * Appends run the engine's write path: rows aligned to the table's schema, given an insertion
 * time, sorted by series and time, deduplicated within the append, and written as one Parquet file
 * per time partition, then committed with the last-value index and the head updated. Timestamps
 * in rows are Dates, ISO strings, or epoch milliseconds (millisecond precision); pass an Arrow
 * IPC stream for microseconds.
 */
export class TableWriter {
  readonly location: string
  readonly #writer: WasmWriter

  /**
   * @param store where the warehouse's objects are
   * @param warehouse the warehouse root as table metadata records it, e.g. `s3://bucket/lake`
   * @param table `namespace.table`, or `table` in the namespace `default`
   */
  constructor(store: Store, warehouse: string, table: string) {
    ensureWasm()
    const base = trimTrailingSlashes(warehouse)
    this.location = tableLocation(base, table)
    const root = rootOf(base)
    const key = (uri: string): string => {
      if (!uri.startsWith(root + "/")) throw new Error(`${uri} is outside ${root}`)
      return uri.slice(root.length + 1)
    }
    const upload = store.upload?.bind(store)
    const host = {
      get: (uri: string) => store.get(key(uri)),
      getRange: (uri: string, start: number, length: number) => store.getRange(key(uri), start, length),
      exists: async (uri: string) => (store.exists ? store.exists(key(uri)) : (await store.get(key(uri))) !== null),
      put: (uri: string, body: Uint8Array) => store.put(key(uri), body),
      putIfAbsent: (uri: string, body: Uint8Array) => store.putIfAbsent(key(uri), body),
      list: async (prefix: string) =>
        (await store.list(key(prefix))).map((o) => ({ uri: `${root}/${o.key}`, size: o.size })),
      delete: (uris: string[]) => store.delete(uris.map(key)),
      ...(upload ? { upload: (uri: string) => upload(key(uri)) } : {}),
    }
    this.#writer = new WasmWriter(host, this.location)
  }

  /** Appends rows (objects, or an Arrow IPC stream) as one commit. */
  async append(
    rows: readonly Record<string, unknown>[] | Uint8Array,
    options: AppendRowsOptions = {},
  ): Promise<AppendReport> {
    const report: unknown =
      rows instanceof Uint8Array
        ? await this.#writer.appendIpc(rows, options)
        : await this.#writer.appendJson(rowsJson(rows), options)
    return report as AppendReport
  }

  /**
   * Runs maintenance on the newest version: retention, compaction of small files (up to
   * `maxBytes` read), and snapshot expiry, as the table's settings say. Appends already run it
   * inline; call this from a scheduled Worker to catch up.
   */
  async maintain(options: MaintainOptions = {}): Promise<MaintainReport> {
    return (await this.#writer.maintain(options)) as MaintainReport
  }

  /** The table's columns: name, Arrow type, and whether a value is required. */
  async schema(): Promise<{ name: string; type: string; required: boolean }[]> {
    const fields = (await this.#writer.schema()) as [string, string, boolean][]
    return fields.map(([name, type, required]) => ({ name, type, required }))
  }
}
