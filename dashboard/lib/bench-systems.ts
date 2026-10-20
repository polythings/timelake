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
