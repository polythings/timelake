/** How the benchmark pages name and color systems, and the shape of a ranking: shared by server and client code. */

/** How the site names each system and reader pair; a system's own reader comes first. */
export const ENGINES: Record<string, string> = {
  "timelake+timelake": "Timelake",
  "timelake+duckdb": "Timelake tables + DuckDB",
  "iceberg+duckdb": "Iceberg (PyIceberg) + DuckDB",
  "s3tables+duckdb": "S3 Tables + DuckDB",
  "lance+duckdb": "Lance + DuckDB",
  "lance+datafusion": "Lance + DataFusion",
  "parquet+duckdb": "Parquet files + DuckDB",
  "parquet+datafusion": "Parquet files + DataFusion",
  "timescaledb+postgres": "TimescaleDB",
  "clickhouse+clickhouse": "ClickHouse",
  "questdb+questdb": "QuestDB",
  "influxdb3+influxdb3": "InfluxDB 3 Core",
  "influxdb3-defaults+influxdb3": "InfluxDB 3 Core, as shipped",
}

export const SYSTEMS: Record<string, string> = {
  timelake: "Timelake",
  iceberg: "Iceberg (PyIceberg)",
  s3tables: "S3 Tables (PyIceberg)",
  lance: "Lance",
  parquet: "Parquet files",
  timescaledb: "TimescaleDB",
  clickhouse: "ClickHouse",
  questdb: "QuestDB",
  influxdb3: "InfluxDB 3 Core",
  "influxdb3-defaults": "InfluxDB 3 Core, as shipped",
}

/** Each system's color in charts, fixed so it never changes with what else is shown. */
const SYSTEM_COLORS: Record<string, string> = {
  timelake: "var(--chart-1)",
  iceberg: "var(--chart-3)",
  lance: "var(--chart-4)",
  parquet: "var(--chart-5)",
  s3tables: "var(--chart-2)",
  timescaledb: "var(--chart-6)",
  clickhouse: "var(--chart-7)",
  questdb: "var(--chart-8)",
  influxdb3: "var(--chart-9)",
  // Runs before the default/tuned split had InfluxDB as shipped as a system of its own.
  "influxdb3-defaults": "var(--chart-9)",
}

/** Timelake read by its own reader: the entry the pages compare the others with. */
export const TIMELAKE_ENGINE = "timelake+timelake"

/** The color of anything the site has no color for, so it never passes for another system. */
const NEUTRAL_COLOR = "var(--muted-foreground)"

/** An engine's key, "system+reader", from a record of either. */
export const engineOf = (r: { system: string; reader: string }) => `${r.system}+${r.reader}`

/** The system of "system+reader"; a system's own key passes through. */
export const systemOf = (engine: string) => engine.split("+", 1)[0] ?? engine

export const engineLabel = (engine: string) => ENGINES[engine] ?? engine

export const systemLabel = (system: string) => SYSTEMS[system] ?? system

/** A system's chart color, from "system" or "system+reader"; a neutral gray for systems the site doesn't know. */
export const systemColor = (key: string) => SYSTEM_COLORS[systemOf(key)] ?? NEUTRAL_COLOR

/** Systems in the order the pages list them: the order of ENGINES. */
export const SYSTEM_ORDER = [...new Set(Object.keys(ENGINES).map(systemOf))]

/** Where `key` sorts in `order`; keys not in it go last. */
const position = (order: string[], key: string) => {
  const i = order.indexOf(key)
  return i < 0 ? order.length : i
}

/** Sort keys: a system's place in SYSTEM_ORDER, an engine's in ENGINES. Unknown ones sort last. */
export const systemOrder = (system: string) => position(SYSTEM_ORDER, system)
export const engineOrder = (engine: string) => position(Object.keys(ENGINES), engine)

/** What a system is in the benchmarks: one line, how it's written, and its maintenance (bench/README.md, "Systems"). */
export type SystemInfo = {
  summary: string
  writes: string
  maintenance: string
  /** When its maintenance happens, in words: while it ingests, as a separate step after, or not at all. */
  when: string
  /**
   * The same as `when` for the pages' logic: `inline` while it ingests, `separate` as a step after it, `managed` by the
   * store on its own schedule, `none` without any.
   */
  maintains: "inline" | "separate" | "managed" | "none"
  /** What its tuned configuration adds to its default one (D23), where it has both. */
  tuned?: string
  readers: string[]
}

export const SYSTEM_INFO: Record<string, SystemInfo> = {
  timelake: {
    summary: "Timelake's own writer: Iceberg tables of sorted Parquet, compacted as it appends.",
    writes: "Table.append per commit, with inline maintenance (compaction, dedup, retention).",
    maintenance: "timelake maintain: each partition rewritten into one sorted file, rollups refreshed.",
    when: "While it ingests: each append compacts what it can. The step after ingest only finishes what's left.",
    maintains: "inline",
    tuned: "The workload's rollups, and its queries in Timelake's own functions where the docs write them that way.",
    readers: ["timelake", "duckdb"],
  },
  iceberg: {
    summary: "Plain Iceberg tables written by PyIceberg appends: what a Firehose stream into S3 Tables writes.",
    writes: "PyIceberg append per commit, tables partitioned by day.",
    maintenance: "A bin-pack rewrite of small files, S3 Tables' default compaction.",
    when: "A separate job after ingest; until it runs, readers see every small file.",
    maintains: "separate",
    tuned: "A sort order on the series key and time, and compaction that sorts each day by it.",
    readers: ["duckdb"],
  },
  s3tables: {
    summary: "The same appends into Amazon S3 Tables, AWS's managed Iceberg.",
    writes: "PyIceberg append per commit, through S3 Tables' Iceberg REST endpoint.",
    maintenance: "S3 Tables' own managed compaction; the run waits for it, up to 6 hours.",
    when: "Managed by AWS, hours after ingest.",
    maintains: "managed",
    tuned: "The tables' sort order on the series key and time, and its compaction set to sort by it.",
    readers: ["duckdb"],
  },
  lance: {
    summary: "Lance datasets, appended commit by commit.",
    writes: "write_dataset in append mode per commit.",
    maintenance: "compact_files, then cleanup_old_versions.",
    when: "A separate job after ingest.",
    maintains: "separate",
    tuned: "Its maintenance, then a B-tree index on the series key and a zone map on time. Default: no maintenance.",
    readers: ["duckdb", "datafusion"],
  },
  parquet: {
    summary: "Parquet files without a table format: a new file per commit, as an S3 sink writes them.",
    writes: "A Parquet file per commit in a folder per day.",
    maintenance: "Each day rewritten into one sorted file.",
    when: "A separate job after ingest.",
    maintains: "separate",
    tuned: "Its maintenance. Default: the files as written, no maintenance.",
    readers: ["duckdb", "datafusion"],
  },
  timescaledb: {
    summary: "TimescaleDB, a PostgreSQL database for time series, on its server's own disk.",
    writes: "COPY per commit into hypertables; rows that arrive twice are skipped.",
    maintenance: "Every chunk converted to its columnstore.",
    when: "A separate step after ingest (in production, a background policy on older chunks).",
    maintains: "separate",
    tuned:
      "An index on (series, time), the columnstore, and continuous aggregates its tuned queries read. Default: the hypertable as created, row store.",
    readers: ["postgres"],
  },
  clickhouse: {
    summary: "ClickHouse MergeTree tables on its server's disk, ordered by series key and time.",
    writes: "Arrow inserts per commit; tables where rows can repeat use ReplacingMergeTree.",
    maintenance: "OPTIMIZE TABLE … FINAL merges every partition; repeated rows are read with FINAL until then.",
    when: "Merges run in the background while it ingests; the step after ingest forces the final merge.",
    maintains: "separate",
    tuned:
      "Ordered by (series, time), partitioned by month, merged after ingest, and materialized views where rows don't repeat. Default: ordered by time.",
    readers: ["clickhouse"],
  },
  questdb: {
    summary: "QuestDB WAL tables partitioned by day, on its server's disk.",
    writes: "Its wire protocol from Python, per commit; repeated rows are removed on write (DEDUP UPSERT KEYS).",
    maintenance: "None; a checkpoint waits until every commit is applied.",
    when: "None: commits are applied in the background, and the step after ingest waits for them.",
    maintains: "none",
    tuned:
      "The series symbol indexed, materialized views, and LATEST ON for the latest rows. Default: the tables as created.",
    readers: ["questdb"],
  },
  influxdb3: {
    summary: "InfluxDB 3 Core with its query file limit raised, as its docs allow, so long time ranges can be queried.",
    writes: "Line protocol over HTTP, four requests at a time.",
    maintenance: "None: Core has no compaction.",
    when: "None.",
    maintains: "none",
    tuned: "The query file limit raised and a last value cache. Default: as shipped, a query reads at most 432 files.",
    readers: ["influxdb3"],
  },
  "influxdb3-defaults": {
    summary:
      "InfluxDB 3 Core as shipped: a query reads at most 432 Parquet files, about 3 days of data, so longer ones fail.",
    writes: "Line protocol over HTTP, four requests at a time.",
    maintenance: "None: Core has no compaction.",
    when: "None.",
    maintains: "none",
    readers: ["influxdb3"],
  },
}

/** Readers by name. */
export const READERS: Record<string, string> = {
  timelake: "Timelake",
  duckdb: "DuckDB",
  datafusion: "DataFusion",
  postgres: "PostgreSQL",
  clickhouse: "ClickHouse",
  questdb: "QuestDB",
  influxdb3: "InfluxDB",
}

/** What reads a system's tables, by reader. */
export const READER_INFO: Record<string, string> = {
  timelake: "Timelake's own reader (the CLI, on DataFusion), with dedup on read, its indexes and its cache.",
  duckdb: "DuckDB reading the files directly with its Iceberg, Lance or Parquet support: no Timelake software.",
  datafusion: "Apache DataFusion reading the files directly, from Python.",
  postgres: "SQL over PostgreSQL's protocol.",
  clickhouse: "ClickHouse's own SQL over HTTP.",
  questdb: "QuestDB's SQL over the PostgreSQL protocol.",
  influxdb3: "InfluxDB's SQL over HTTP.",
}

/** Pairs whose meaning isn't just the system's and the reader's. */
const ENGINE_NOTES: Record<string, string> = {
  "timelake+duckdb":
    "Timelake's tables read by plain DuckDB, with no Timelake software: it sees rows that arrived twice until compaction removes them.",
}

/** One or two sentences on an entry (a system, or a system and reader pair), for a hover. */
export function entryDescription(key: string): string {
  const system = systemOf(key)
  const reader = key.slice(system.length + 1)
  const info = SYSTEM_INFO[system]
  if (ENGINE_NOTES[key]) return ENGINE_NOTES[key]
  if (!info) return engineLabel(key)
  if (!reader || isOwnReader(key)) return info.summary
  return `${info.summary} Read by ${READERS[reader] ?? reader}.`
}

/** What a reader of a system's tables is: the pair's own note where it has one, else the reader's. */
export const readerDescription = (system: string, reader: string) =>
  ENGINE_NOTES[`${system}+${reader}`] ?? READER_INFO[reader] ?? reader

/** The anchor of a system's entry in a benchmark page's list of entries. */
export const entryAnchor = (key: string) => `entry-${systemOf(key)}`

/**
 * A system's own reader, as an engine: the first of the system's engines in ENGINES (what timebench lists first in a
 * run's readers). Undefined for a system the site doesn't know.
 */
export const ownEngine = (system: string) => Object.keys(ENGINES).find((e) => systemOf(e) === system)

/** Whether `engine` is its system's own reader. */
export const isOwnReader = (engine: string) => ownEngine(systemOf(engine)) === engine

export type Rank = {
  key: string
  label: string
  system: string
  value: number
  timelake: boolean
  /** Of the queries ranked, how many answers didn't match the reference. */
  wrong?: number
  /** For query times: the best repeat, and a first run's requests, their price and the bytes it read (averaged over
   * the queries; on object storage only). */
  repeat?: number
  requests?: number
  usd?: number
  bytes?: number
}
export type Ranking = {
  /** What it ranks, for code to find it by: e.g. `queries-maintained`, `load`, `commit`, or a query's name. */
  id: string
  title: string
  note: string
  unit: "seconds" | "milliseconds" | "rows/s" | "requests" | "usd" | "bytes"
  /** Rows come best first; for rates, higher is better. */
  higherIsBetter?: boolean
  /** How many queries each value is over (a geometric mean), where it is one. */
  queries?: number
  rows: Rank[]
}
