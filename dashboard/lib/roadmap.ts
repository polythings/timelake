/** The public roadmap at /roadmap. A plain-words view of SPEC.md §19 and §21: change them together. */

export type RoadmapItem = {
  title: string
  description: string
  /** Only items under "Now" have one. */
  status?: "done" | "next"
}

export const ROADMAP_UPDATED = "October 2026"

export const NOW: RoadmapItem[] = [
  {
    title: "Benchmarks over time",
    description: "Every run kept with its commit, and a weekly run on a fixed machine, charted on the benchmarks page.",
    status: "next",
  },
  {
    title: "Industrial IoT benchmark",
    description: "Timelake against InfluxDB, TimescaleDB, ClickHouse, and QuestDB, with the monthly cost of each.",
    status: "next",
  },
  {
    title: "Time-weighted averages",
    description: "Means of irregular readings, each weighted by how long it held. Exact, from raw data or rollups.",
    status: "done",
  },
  {
    title: "Rollups answer more queries",
    description:
      "Daily queries from minute rollups, rollups computed from finer ones, and each bucket's first, lowest, highest, and last readings.",
    status: "done",
  },
  {
    title: "Faster reads from object storage",
    description:
      "A query's reads go out together, in parts of 1 MiB. First queries on S3-like storage take a third less time.",
    status: "done",
  },
  {
    title: "Hundreds of thousands of series",
    description: "Ingest and dashboards measured at 400,000 series: about 2 million rows a second on a laptop.",
    status: "done",
  },
  {
    title: "Write from Cloudflare Workers",
    description:
      "Append to tables and keep them compacted from Workers and other JavaScript runtimes, with the WebAssembly library.",
    status: "done",
  },
  {
    title: "Compaction in fixed memory",
    description:
      "Compaction merges sorted files as streams, so its memory no longer grows with the partition: 4× less memory, 3× faster.",
    status: "done",
  },
  {
    title: "Install from crates.io, PyPI, and npm",
    description: "Every release on the usual registries, with installers and binaries for Linux, macOS, and Windows.",
    status: "done",
  },
  {
    title: "Faster first queries",
    description:
      "A table's state, file footers included, is read in one request. First queries take a third to half the time.",
    status: "done",
  },
  {
    title: "Faster commits",
    description: "Everything a commit needs is uploaded at once, then published in one step: twice as fast.",
    status: "done",
  },
  {
    title: "Open tables on any object store",
    description:
      "Parquet and Iceberg on S3, R2, GCS, Azure, or a disk. Use them from the CLI, Python, a browser, or DuckDB.",
    status: "done",
  },
  {
    title: "No maintenance jobs",
    description: "Compaction, deduplication, retention, and cleanup run inside the writer.",
    status: "done",
  },
  {
    title: "Latest value of every series",
    description: "Answered from a small index in a few requests, not a scan.",
    status: "done",
  },
  {
    title: "Rows sent twice count once",
    description:
      "Timelake's reader keeps only the latest write of each reading, before compaction. Each row records when it arrived.",
    status: "done",
  },
  {
    title: "Local cache",
    description:
      "Repeat queries read their data from memory or local disk instead of the bucket: about 3× faster on S3.",
    status: "done",
  },
  {
    title: "Skip version checks",
    description:
      "Queries can reuse a table version for a set time instead of checking for a newer one, so warm queries send no requests.",
    status: "done",
  },
  {
    title: "Rollups",
    description:
      "Hourly and daily summaries that stay up to date as data lands. Matching queries read them, with the same answer as the raw data.",
    status: "done",
  },
  {
    title: "Settings for every feature",
    description:
      "Turn each feature on or off, or tune it, per table, per session or per call, from one list of settings.",
    status: "done",
  },
]

export const COMING_UP: RoadmapItem[] = [
  {
    title: "Tuned on real data",
    description:
      "A beta on a production industrial dataset with parallel writers fed from Kinesis: throughput, latency, and cost measured, then tuned for.",
  },
  {
    title: "Side-by-side demos",
    description: "The same queries on Timelake and other engines, with time and a running cost counter.",
  },
  {
    title: "More rollups",
    description: "Percentiles in rollups.",
  },
  {
    title: "Time-series functions",
    description: "Gap filling, counters, and percentiles.",
  },
  {
    title: "Any engine can attach",
    description: "An Iceberg REST catalog, so Snowflake, Trino, Athena, and Spark query tables in place.",
  },
  { title: "Deletes and GDPR erasure", description: "Audited deletes you can undo, and purges within 7 days." },
  {
    title: "Import and backfill",
    description: "Bring history from TimescaleDB, Iceberg, or S3 Tables, and publish it all at once.",
  },
  { title: "Seconds-fresh data", description: "New rows visible within a second, before they are committed." },
  { title: "Ingestion with rules", description: "MQTT, HTTP, and Kafka in, with rules and alerts as data arrives." },
  { title: "DuckDB extension", description: "Timelake's speedups and functions inside DuckDB." },
]

export const EXPLORING: RoadmapItem[] = [
  { title: "Agent access", description: "AI agents query your data within the permissions you set." },
  {
    title: "AI agent benchmark",
    description: "Do AI agents answer questions about your data better, and for less, when it's in Timelake?",
  },
  { title: "Dashboard read models", description: "Small ready-made files for frontends, refreshed on every write." },
  { title: "Data to events", description: "Turn new data into events on any stream." },
  { title: "Computed metrics", description: "Your Python functions, applied automatically like rollups." },
  { title: "Edge and device sync", description: "Write on devices and gateways; Timelake moves it to the lake." },
  {
    title: "Server mode",
    description:
      "Timelake as a database on one machine, with the bucket as the copy. Then TSBS run with its own tools, next to published results.",
  },
]
