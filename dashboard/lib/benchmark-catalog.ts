/**
 * Every benchmark we run, plan to run, or decided not to run, with how we run it (spec 44, spec 46). Keep in sync
 * with spec 44 (spec/0044-timebench.md). A benchmark with a `workload` has results from timebench (bench/timebench/workloads).
 */

export type Status = "running" | "planned" | "not planned"

export type Section = {
  title: string
  text?: string[]
  items?: string[]
  table?: { head: string[]; rows: string[][] }
}

export type Benchmark = {
  id: string
  name: string
  /** The benchmark's own definition; null for ours. */
  origin: { label: string; url: string } | null
  status: Status
  /** What it measures, in one line. */
  summary: string
  /** Why it has this status. */
  why: string
  /** The timebench workload that produces its results. */
  workload?: string
  /**
   * What comes from the original benchmark and what is ours, shown next to its results and first in its methodology,
   * so no one assumes we ran the original's own tools when we didn't.
   */
  provenance?: string
  methodology?: Section[]
}

const TSBS = { label: "TSBS on GitHub", url: "https://github.com/timescale/tsbs" }

export const BENCHMARKS: Benchmark[] = [
  {
    id: "tsbs-iot",
    name: "TSBS IoT",
    origin: TSBS,
    status: "running",
    summary: "A truck fleet's location and diagnostics, streamed in and queried like a fleet dashboard.",
    why: "Our base workload. TSBS is the benchmark time-series databases publish numbers for.",
    workload: "tsbs-iot",
    provenance:
      "We use TSBS's data generator, unchanged, and its 12 query types, ported to SQL every system runs. We don't use TSBS's loaders or query runners: timebench loads the data and times the queries, so the numbers don't compare with published TSBS results.",
    methodology: [
      {
        title: "The original",
        text: [
          "The Time Series Benchmark Suite (TSBS), by Timescale, generates data for a use case, loads it into a database, and runs a fixed set of query types. Its IoT use case models a fleet of trucks: a readings table (location, speed, fuel use) and a diagnostics table (fuel level, load, status), each truck described by tags.",
          "TSBS measures bulk-load throughput and query throughput with many parallel workers against a database server. We keep its data and queries, and change how data arrives and what we time, because a lake receives data in small batches and is read by short-lived processes.",
        ],
      },
      {
        title: "Data",
        items: [
          "TSBS's own generator, `tsbs_generate_data --use-case=iot`, seed 123, one reading every 10 seconds per truck from 2026-01-01 to 2026-01-04.",
          "Standard scale: 500 trucks, 11.7M readings and 11.7M diagnostics. The generator leaves about 10% of points missing and delivers some late, on purpose.",
          "Converted once to Parquet with the same schema TSBS loads into TimescaleDB: `readings`, `diagnostics`, and a `tags` table.",
        ],
      },
      {
        title: "How the data arrives",
        text: [
          "Rows are streamed in 5-minute windows, the default buffer interval of Amazon Data Firehose: 864 windows, one commit per table per window, 1,728 commits of about 13,500 rows. Every system gets identical rows in the same order. The tags table is loaded once first.",
        ],
      },
      {
        title: "Queries",
        items: [
          "The 12 TSBS IoT query types: last location, low fuel, high load, stationary trucks, long driving sessions, long daily sessions, average versus projected fuel use, average daily driving duration and session, average load, daily activity, breakdown frequency.",
          "Ported once from TSBS's TimescaleDB SQL to SQL every engine runs. Random parameters are fixed (fleet South, windows near the end of the data). TimescaleDB's `LATERAL … LIMIT 1` becomes `ROW_NUMBER() … = 1`. Time buckets are placeholders filled per engine. Breakdown frequency counts the intended fraction of rows with status 0: TSBS's version always returns 1.",
          "5 more queries about one truck (its last hour, all its history, its latest row, its hourly averages) and the fleet's last hour: what a dashboard asks most.",
        ],
      },
      {
        title: "Systems",
        table: {
          head: ["System", "Writes", "Maintenance", "Read by"],
          rows: [
            ["Timelake", "Table.append per commit", "inline, plus Table.maintain()", "Timelake; DuckDB"],
            [
              "Iceberg (PyIceberg)",
              "append per commit, partitioned by day",
              "rewrite into larger files, as S3 Tables does by default",
              "DuckDB",
            ],
            ["S3 Tables", "the same appends through its Iceberg endpoint", "its own managed compaction", "DuckDB"],
            ["Lance", "write_dataset in append mode", "compact_files, cleanup_old_versions", "DuckDB; DataFusion"],
            ["Parquet files", "a new file per commit, folders by day", "one sorted file per day", "DuckDB; DataFusion"],
            ["TimescaleDB", "COPY per commit into hypertables", "convert chunks to the columnstore", "PostgreSQL"],
          ],
        },
      },
      {
        title: "Where it runs",
        items: [
          "AWS S3: tables in an S3 Standard bucket in us-east-2. The weekly runs read and write from an instance in the same region. The runs of 2026-10-13 came from a laptop about 45 ms away, so their times include that trip for every system.",
          "Local disk: tables on the machine's own SSD. The operating system's file cache stays warm between queries.",
          "Tiger Cloud: TimescaleDB on Tiger Cloud's smallest tier (shared CPU), loaded in one go rather than streamed, queried from a laptop with a 30 ms round trip. On the other storage, TimescaleDB runs in a container on the benchmark machine.",
          "S3 Tables runs only on AWS, in its own table bucket. Its compaction runs on AWS's schedule, hours after ingest; the run waits for it before measuring after maintenance.",
        ],
      },
      {
        title: "What the results show",
        items: [
          "Query time, right after ingest and after maintenance: the geometric mean of first runs over the queries every system completed.",
          "Commit time: the median and 95th percentile of the 1,728 commits.",
          "While ingesting: every 15 seconds a fresh reader runs the 5 single-truck and fleet queries (the dashboard) and checks the newest row it sees.",
          "Concurrent dashboards: 1, 4, and 16 clients running the dashboard queries for 30 seconds each, after maintenance.",
          "Cost on AWS: ingest requests per month at this data rate, storage per month, and a million queries, at S3 list prices.",
          "Storage after maintenance: files and bytes.",
          "On S3: requests and bytes per query and per phase.",
        ],
      },
      {
        title: "Compared with published TSBS numbers",
        text: [
          "Published TSBS results measure a database server loading data as fast as it can and answering many queries in parallel. Ours measure single queries from a fresh process after streaming commits, so the numbers can't be set side by side.",
          "Planned: a run with TSBS's own loader and query runner, once Timelake has a server mode that speaks the PostgreSQL protocol. Those numbers can sit next to published ones.",
        ],
      },
    ],
  },
  {
    id: "tsbs-devops",
    name: "TSBS DevOps",
    origin: TSBS,
    status: "running",
    summary: "Servers' CPU metrics every 10 seconds, queried like a monitoring dashboard.",
    why: "The TSBS use case most databases publish numbers for.",
    workload: "tsbs-devops",
    provenance:
      "We use TSBS's data generator, unchanged, and its 15 query types, ported to SQL every system runs. We don't use TSBS's loaders or query runners: timebench loads the data and times the queries, so the numbers don't compare with published TSBS results.",
    methodology: [
      {
        title: "The original",
        text: [
          "TSBS's DevOps use case simulates servers reporting metrics every 10 seconds. Its cpu-only variant keeps the CPU metrics, 10 per host, and that's the one most databases publish results for: all 15 DevOps query types read only the cpu table.",
          "The queries range from one host's maximum CPU over an hour to averages per host and hour across all hosts, the moments a host's CPU passed 90%, and the latest reading of every host.",
        ],
      },
      {
        title: "Data",
        items: [
          "TSBS's own generator, `tsbs_generate_data --use-case=cpu-only`, seed 123, one reading every 10 seconds per host from 2026-01-01 to 2026-01-04.",
          "Standard scale: 4,000 hosts, 104M rows, the size published TSBS results use. Small scale: 100 hosts, for development.",
          "Converted once to Parquet with the schema TSBS loads into TimescaleDB: a `cpu` table with the host name in it, as TSBS's loader puts it, and a `tags` table describing each host.",
        ],
      },
      {
        title: "Two ways the data arrives",
        items: [
          "Streamed: 5-minute windows like TSBS IoT, 864 commits of 120,000 rows at standard scale. Queries run after this.",
          "Bulk load: 1-hour windows, 72 commits of 1.44M rows, as fast as each system commits. The rate in rows per second is the number closest to TSBS's own load results, though TSBS loads with many parallel workers and timebench with one writer per system.",
        ],
      },
      {
        title: "Queries",
        items: [
          "All 15 types, named as in TSBS: single-groupby-<metrics>-<hosts>-<hours> (6 combinations), cpu-max-all-1 and -8, double-groupby-1, -5 and -all, high-cpu-1 and -all, lastpoint, groupby-orderby-limit.",
          "Ported once from TSBS's TimescaleDB SQL to SQL every engine runs. Random hosts and time windows are fixed (hosts below 100, windows ending at 2026-01-03 23:00, TSBS's window lengths). TimescaleDB's `DISTINCT ON` (lastpoint) becomes `ROW_NUMBER() … = 1`.",
          "The dashboard queries, probed during ingest and run by concurrent clients, are the single-host ones: three single-groupby types, cpu-max-all-1, high-cpu-1, and groupby-orderby-limit.",
        ],
      },
      {
        title: "Systems and storage",
        text: [
          "The same systems, readers, and storage as TSBS IoT (see its methodology), with the host name as the series key: Timelake sorts by it, and TimescaleDB indexes it.",
        ],
      },
      {
        title: "Compared with published TSBS numbers",
        text: [
          "Published TSBS DevOps results come from a database server queried by many parallel workers, reporting queries per second at full load. Ours are single queries from a fresh process, so the query times can't be set side by side. The bulk-load rate and the concurrent-dashboard throughput are the closest to TSBS's terms.",
          "Planned: a run with TSBS's own loader and query runner, once Timelake has a server mode that speaks the PostgreSQL protocol.",
        ],
      },
    ],
  },
  {
    id: "clickbench",
    name: "ClickBench",
    origin: { label: "ClickBench", url: "https://benchmark.clickhouse.com" },
    status: "planned",
    summary: "One wide table of web analytics, 100M rows; 43 queries.",
    why: "We run a 10% subset during development. Comparable numbers need the full dataset on its reference machine.",
    provenance:
      "We'll use ClickBench's dataset, queries, and scripts, by its rules, for an official entry. Until then, a 10% subset runs in development and isn't published.",
    methodology: [
      {
        title: "The original",
        text: [
          "ClickHouse's benchmark of analytical databases: 43 queries on a 100M-row table of web hits, each run cold and then twice hot, with load time and data size. Its public leaderboard includes DataFusion and DuckDB over Parquet and several data-lake setups.",
        ],
      },
      {
        title: "How we'll run it",
        items: [
          "The full dataset, written by Timelake into a table on S3 and on local disk.",
          "ClickBench's rules on its reference machine (c6a.4xlarge): a cold run and two hot runs per query, load time, size on disk.",
          "Its queries and scripts are licensed CC BY-NC-SA, so they're downloaded at a pinned version rather than kept in our repository.",
          "An official leaderboard entry once Timelake is more refined (issue #90).",
        ],
      },
    ],
  },
  {
    id: "rtabench",
    name: "RTABench",
    origin: { label: "RTABench", url: "https://rtabench.com" },
    status: "planned",
    summary: "Orders and their events: selective lookups, joins, and pre-aggregated queries.",
    why: "Close to fresh-data dashboards. Its pre-aggregated queries wait for rollups.",
    provenance:
      "We'll use its data and queries; timebench will load the data and time the queries, so the numbers won't compare with its leaderboard.",
    methodology: [
      {
        title: "The original",
        text: [
          "Timescale's real-time analytics benchmark: an online store's customers, products, orders, and 171M order events, with 33 queries. Some read one order or a short window, some join tables, some read pre-aggregated views. It uses ClickBench's harness and rules.",
        ],
      },
      {
        title: "How we'll run it",
        items: [
          "Its data written into each system, the events streamed like TSBS IoT.",
          "The lookup and join queries as published. The pre-aggregated queries once Timelake has rollups, reported as unsupported until then.",
        ],
      },
    ],
  },
  {
    id: "tsm-bench",
    name: "TSM-Bench",
    origin: { label: "TSM-Bench on GitHub", url: "https://github.com/eXascaleInfolab/TSM-Bench" },
    status: "planned",
    summary: "Sensor data: 7 query types, queries while data streams in, compression.",
    why: "Its queries and online mode fit. Its code has no license yet; we're asking the authors.",
    provenance: "We'll use its data and query templates once its license allows; timebench will run them.",
    methodology: [
      {
        title: "The original",
        text: [
          "A benchmark from the University of Fribourg (VLDB 2023) on hydrology sensor data extended by a generator, up to 200,000 series. Seven query types: selection, filtering, aggregation, downsampling, upsampling, the average of two series, and correlation. An online mode inserts at a fixed rate while querying.",
        ],
      },
      {
        title: "How we'll run it",
        items: [
          "Its datasets and query templates, once the license allows.",
          "Its online mode as a timebench run that queries while ingest continues, reporting query latency against ingest rate.",
          "Compression as bytes on storage per data point.",
        ],
      },
    ],
  },
  {
    id: "iot-benchmark",
    name: "IoT-benchmark",
    origin: { label: "IoT-benchmark on GitHub", url: "https://github.com/thulab/iot-benchmark" },
    status: "planned",
    summary: "Batched writes with out-of-order data and many clients; 12 query types; read/write mixes.",
    why: "The best tool for mixed reads and writes. Its adapters talk to a server, so it waits for Timelake's server mode.",
    provenance: "We'll run its own tool, unchanged, through a Timelake adapter for its Java interface.",
    methodology: [
      {
        title: "The original",
        text: [
          "The benchmark of Apache IoTDB, by Tsinghua University: devices with sensors write batches from many clients, with a set share of out-of-order points, while clients run 12 query types (point, range, value filter, aggregates, group by time, latest point). A ratio sets the mix of writes and each query type. It supports IoTDB, InfluxDB, TimescaleDB, QuestDB, TDengine, and others.",
        ],
      },
      {
        title: "How we'll run it",
        items: [
          "Through a Timelake adapter for its Java interface, talking to Timelake's server mode (PostgreSQL protocol or Flight SQL).",
          "The same settings for every system: batch size, clients, out-of-order share, and the read/write mix.",
        ],
      },
    ],
  },
  {
    id: "longevity",
    name: "Longevity",
    origin: { label: "LST-Bench on GitHub", url: "https://github.com/microsoft/lst-bench" },
    status: "planned",
    summary: "How queries slow down over weeks of commits, and what maintenance wins back.",
    why: "Borrows LST-Bench's method (Microsoft, SIGMOD 2024), applied to our own time-series data.",
    provenance: "Only LST-Bench's method: our own data, queries, and runs, not its code or workload.",
    methodology: [
      {
        title: "The idea",
        text: [
          "LST-Bench measures how table formats degrade over many rounds of writes and maintenance. We apply the method to time series: rounds of streamed commits, with each system's maintenance between some rounds, and the same queries after every round.",
        ],
      },
      {
        title: "How we'll run it",
        items: [
          "TSBS IoT and, later, the industrial IoT workload, extended to 30 days of commits.",
          "Query time, files, metadata size, and requests after each round, charted against the number of commits.",
        ],
      },
    ],
  },
  {
    id: "industrial-iot",
    name: "Industrial IoT",
    origin: null,
    status: "running",
    summary: "Plant data as it really arrives, from a day to a month, against databases and lakes, with monthly cost.",
    why: "TSBS models trucks every 10 seconds. Plants look different, and that's where Timelake is used.",
    workload: "iiot",
    provenance: "Our own benchmark: the data generator, the queries, and the method are all timebench's.",
    methodology: [
      {
        title: "Data",
        text: [
          "Our own generator, the same rows from the same seed every time: a plant of lines and machines, 10 tags per machine. Pressure every second; flow, temperature, current, and speed every 10 seconds; vibration and an energy counter every minute; machine state, alarm, and setpoint when they change.",
          "Machines run, stop, and fault. Pressure sometimes goes over its alarm limit, energy meters get replaced, sensors drop out or get stuck. Every event is recorded, so answers can be checked.",
        ],
        table: {
          head: ["Scale", "Tags", "Raw data", "History"],
          rows: [
            ["small", "1,000", "about 10 MB an hour", "30 days"],
            ["standard", "10,000", "about 100 MB an hour", "30 days"],
            ["large", "100,000", "about 1 GB an hour", "30 days"],
            ["xlarge", "1,000,000", "about 10 GB an hour", "7 days"],
          ],
        },
      },
      {
        title: "How the data arrives",
        text: [
          "Through gateways, one per line. A gateway goes offline for 1 to 12 hours now and then; when it's back, it sends its backlog, an hour per 5 minutes, while live data keeps coming. It also resends the last 5 minutes before it went offline, and 1.5% of all rows arrive twice. Every tenth line is wired and never drops out.",
          "History is loaded in bulk, an hour of arrivals per commit. The last day arrives live, a 5-minute window per commit.",
        ],
      },
      {
        title: "Queries",
        items: [
          "Dashboards: the latest value of 1,000 tags; the last 15 minutes of two machines; a 30-day trend as hourly min, max, and mean.",
          "Investigation: every tag of five machines around an alarm two hours ago.",
          "Reports: a shift report per machine joined with the asset model; daily counts and means over 30 days.",
          "Process: time above the alarm limit, time-weighted averages, machine uptime, energy from counters that reset, data gaps.",
          "All relative to the newest data, so a query means the same after 1 day, 7 days, or 30.",
        ],
      },
      {
        title: "Right answers",
        text: [
          "A reference copy keeps every row that arrived, once. Each system's answer is compared with it. A system that keeps the duplicates, or doesn't see late rows yet, shows as wrong next to its speed.",
        ],
      },
      {
        title: "Systems",
        items: [
          "Timelake, TimescaleDB, ClickHouse, QuestDB, InfluxDB 3 Core, Iceberg, Lance, and plain Parquet files.",
          "Each database at a pinned version, loaded with its own recommended client, and removing duplicates the way its docs say.",
          "InfluxDB 3 Core by default won't read more than 432 files in one query, about 3 days of data. It runs twice: as shipped, and with the limit raised.",
          "Later: the same on each vendor's managed cloud, at its own prices.",
        ],
      },
      {
        title: "What we report",
        items: [
          "Query time after 1 day, 7 days, and the full history, after maintenance, and while data arrives.",
          "Whether each answer is right.",
          "How fresh data is, load speed, storage, and what each setup costs a month on AWS.",
        ],
      },
    ],
  },
  {
    id: "ai-agents",
    name: "AI agents",
    origin: null,
    status: "planned",
    summary:
      "An AI agent answers questions about the data through tools. Is it right more often, and cheaper, from Timelake?",
    why: "People increasingly ask questions of their data through AI agents, and no public benchmark compares storage for that.",
    provenance: "Our own benchmark. Later, OpenRCA's and AssetOpsBench's tasks, run with their own code.",
    methodology: [
      {
        title: "The question",
        text: [
          "TSRBench and similar benchmarks paste a series into the prompt, so storage plays no part. Here the agent has to find and fetch the data itself. Benchmarks close to this exist for other data (OpenRCA, AssetOpsBench, o11y-bench); none compares where the data is stored.",
        ],
      },
      {
        title: "How we'll run it",
        items: [
          "The same agent, model, and questions against the same data in raw files, DuckDB, TimescaleDB, Timelake over SQL, and Timelake over its own agent tools.",
          "Questions generated from the data with events added on purpose, so every answer is known: values with units, comparisons between periods, anomalies and when they happened, a cause shared by many devices, gaps, late data, and questions that should be refused.",
          "Answers checked automatically. For each question: right or wrong, tokens, tool calls, time, and cost.",
          "Then OpenRCA's and AssetOpsBench's tasks against Timelake, as published comparisons.",
        ],
      },
    ],
  },
  {
    id: "ts-benchmark",
    name: "TS-Benchmark",
    origin: { label: "TS-Benchmark on GitHub", url: "https://github.com/dbiir/TS-Benchmark" },
    status: "not planned",
    summary: "Wind-farm sensor data: loading, appends, and 5 queries (ICDE 2021).",
    why: "Unmaintained since 2022, and its 5 queries are covered by the benchmarks above.",
  },
  {
    id: "tsdbbench",
    name: "TSDBBench",
    origin: { label: "TSDBBench", url: "https://tsdbbench.github.io" },
    status: "not planned",
    summary: "Inserts and range scans on time-series databases, YCSB style.",
    why: "Unmaintained since 2019; the databases it compares have moved on.",
  },
  {
    id: "tpcx-iot",
    name: "TPCx-IoT",
    origin: { label: "TPCx-IoT", url: "https://www.tpc.org/tpcx-iot" },
    status: "not planned",
    summary: "Ingest throughput of 1 KB sensor records.",
    why: "TPC rules allow only audited results to be published, and its key-value records don't suit columnar files.",
  },
  {
    id: "stac-m3",
    name: "STAC-M3",
    origin: { label: "STAC-M3", url: "https://www.stacresearch.com/m3" },
    status: "not planned",
    summary: "Financial tick data analytics.",
    why: "Closed to members, with paid audits.",
  },
  {
    id: "tsrbench",
    name: "TSRBench",
    origin: { label: "TSRBench", url: "https://tsrbench.github.io" },
    status: "not planned",
    summary: "How well AI models reason about time series.",
    why: "It pastes the series into the prompt, so storage plays no part. Our AI agents benchmark asks the question that matters for Timelake.",
  },
]

export const benchmark = (id: string) => BENCHMARKS.find((b) => b.id === id)
