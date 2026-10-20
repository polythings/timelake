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

/**
 * Each system's color in charts, fixed so it never changes with what else is shown. S3 Tables and TimescaleDB share
 * one: they never run on the same target (AWS only, and a database server).
 */
export const SYSTEM_COLORS: Record<string, string> = {
  timelake: "var(--chart-1)",
  iceberg: "var(--chart-3)",
  lance: "var(--chart-4)",
  parquet: "var(--chart-5)",
  s3tables: "var(--chart-2)",
  timescaledb: "var(--chart-2)",
}

export const engineLabel = (engine: string) => ENGINES[engine] ?? engine

/** What a system is in the benchmarks: one line, how it's written, and its maintenance (bench/README.md, "Systems"). */
export type SystemInfo = { summary: string; writes: string; maintenance: string; readers: string[] }

export const SYSTEM_INFO: Record<string, SystemInfo> = {
  timelake: {
    summary: "Timelake's own writer: Iceberg tables of sorted Parquet, compacted as it appends.",
    writes: "Table.append per commit, with inline maintenance (compaction, dedup, retention).",
    maintenance: "timelake maintain: each partition rewritten into one sorted file, rollups refreshed.",
    readers: ["timelake", "duckdb"],
  },
  iceberg: {
    summary: "Plain Iceberg tables written by PyIceberg appends: what a Firehose stream into S3 Tables writes.",
    writes: "PyIceberg append per commit, tables partitioned by day.",
    maintenance: "A bin-pack rewrite of small files, S3 Tables' default compaction.",
    readers: ["duckdb"],
  },
  s3tables: {
    summary: "The same appends into Amazon S3 Tables, AWS's managed Iceberg.",
    writes: "PyIceberg append per commit, through S3 Tables' Iceberg REST endpoint.",
    maintenance: "S3 Tables' own managed compaction; the run waits for it, up to 6 hours.",
    readers: ["duckdb"],
  },
  lance: {
    summary: "Lance datasets, appended commit by commit.",
    writes: "write_dataset in append mode per commit.",
    maintenance: "compact_files, then cleanup_old_versions.",
    readers: ["duckdb", "datafusion"],
  },
  parquet: {
    summary: "Parquet files without a table format: a new file per commit, as an S3 sink writes them.",
    writes: "A Parquet file per commit in a folder per day.",
    maintenance: "Each day rewritten into one sorted file.",
    readers: ["duckdb", "datafusion"],
  },
  timescaledb: {
    summary: "TimescaleDB, a PostgreSQL database for time series, on its server's own disk.",
    writes: "COPY per commit into hypertables; rows that arrive twice are skipped.",
    maintenance: "Every chunk converted to its columnstore.",
    readers: ["postgres"],
  },
  clickhouse: {
    summary: "ClickHouse MergeTree tables on its server's disk, ordered by series key and time.",
    writes: "Arrow inserts per commit; tables where rows can repeat use ReplacingMergeTree.",
    maintenance: "OPTIMIZE TABLE … FINAL merges every partition; repeated rows are read with FINAL until then.",
    readers: ["clickhouse"],
  },
  questdb: {
    summary: "QuestDB WAL tables partitioned by day, on its server's disk.",
    writes: "Its wire protocol from Python, per commit; repeated rows are removed on write (DEDUP UPSERT KEYS).",
    maintenance: "None; a checkpoint waits until every commit is applied.",
    readers: ["questdb"],
  },
  influxdb3: {
    summary: "InfluxDB 3 Core with its query file limit raised, as its docs allow, so long time ranges can be queried.",
    writes: "Line protocol over HTTP, four requests at a time.",
    maintenance: "None: Core has no compaction.",
    readers: ["influxdb3"],
  },
  "influxdb3-defaults": {
    summary:
      "InfluxDB 3 Core as shipped: a query reads at most 432 Parquet files, about 3 days of data, so longer ones fail.",
    writes: "Line protocol over HTTP, four requests at a time.",
    maintenance: "None: Core has no compaction.",
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
  const [system = key, reader] = key.split("+")
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
export const entryAnchor = (key: string) => `entry-${key.split("+", 1)[0] ?? key}`

/** Whether `engine` is its system's own reader: the first of the system's engines above. */
export const isOwnReader = (engine: string) => {
  const system = engine.split("+", 1)[0] ?? engine
  return Object.keys(ENGINES).find((e) => e.startsWith(`${system}+`)) === engine
}

export type Rank = {
  key: string
  label: string
  system: string
  value: number
  timelake: boolean
  /** Of the queries ranked, how many answers didn't match the reference. */
  wrong?: number
  /** For query times: the best repeat, and the requests of a first run (averaged over the queries). */
  repeat?: number
  requests?: number
}
export type Ranking = {
  title: string
  note: string
  unit: "seconds" | "milliseconds" | "rows/s"
  /** Rows come best first; for rates, higher is better. */
  higherIsBetter?: boolean
  /** How many queries each value is over (a geometric mean), where it is one. */
  queries?: number
  rows: Rank[]
}
