# Changelog

Changes users can see, newest first. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
The crates, the CLI, the Python package, the WASM reader and the TypeScript client share one version, which follows
[Semantic Versioning](https://semver.org) as [Cargo applies it](https://doc.rust-lang.org/cargo/reference/semver.html):
before 1.0, a minor release (0.3 → 0.4) may break things, and its entry says how; a patch release never does. Each
version is a [tag on GitHub](https://github.com/polythings/timelake/tags), so its code and benchmarks can be checked out.

## Unreleased

### Added

- Aggregate `m4(time, value)`: a group's first, smallest, largest and last points, as a list of `{time, value}`.
  Rollups store it and answer wider buckets from it (hourly points of 1,000 tags over 30 days: 2.0 → 0.51 s from a
  1-minute rollup). After Apache IoTDB's M4.
- Rollups of rollups: a table's rollups are refreshed narrowest first, and each is computed from a narrower one where
  that is up to date (a daily rollup from the minute rollup: 5.2 → 1.8 s on 30 days of IIoT readings).
- Rollups answer coarser queries: a query whose `time_bucket` is a multiple of the rollup's width (daily from an
  hourly rollup), or that groups by fewer of its columns, is answered by aggregating the rollup's rows again. IIoT,
  30 days, a 1-minute rollup: daily statistics per tag 2.1 → 0.25 s, 6-hourly totals 1.1 → 0.11 s.
- Setting `newest-first` (on by default): `ORDER BY time DESC LIMIT k` reads time partitions newest first and stops
  once older ones can't hold any of the k newest rows (#72). IIoT, 30 days: the 100 newest readings before a past
  time 298 → 38 ms; TSBS IoT on local S3: the 10 newest readings 392 → 320 ms.
- Setting `in-list-pruning` (on by default): `column IN (…)` filters of more than 20 values skip row groups and pages
  whose range holds none of the values, which DataFusion's pruning leaves alone (#103). IIoT, compacted, local disk:
  a 100-tag list 54 → 31 ms, a 21-tag list 53 → 26 ms. On object storage it also prunes with a join's runtime
  filter: IIoT `energy` on local S3 (20 ms per request) 54 → 26 ms repeated, 204 → 183 ms first run.
- Setting `filter-pushdown` (`auto`, `true`, `false`): the Timelake reader applies filters while decoding Parquet and
  decodes the other columns only for rows that pass. `auto`, the default, turns it on for local files (TSBS geomeans
  4–5% faster, selective group-bys up to 40%) and off on object storage, where it adds a round trip per row group.
- Data files use the codec in Iceberg's `write.parquet.compression-codec` and `write.parquet.compression-level`
  (ZSTD 3 by default; `lz4`, `snappy`, `gzip`, `brotli`, `uncompressed`). Before, Timelake always wrote ZSTD 3.

### Changed

- Compaction's merge (also used for appends past 1 GiB) is up to 5× faster on files that alternate series by series,
  such as 5-minute batches of every device: 10 M rows of two such inputs merge in 2.6 s instead of 13.9 s.
- The block cache sends GETs of at most 1 MiB (storage option `timelake_cache_request_bytes`; `0` for no limit),
  longer runs split into parts fetched at once. With 20 ms per request and 50 MiB/s per request, TSBS IoT first
  queries 126 → 115 ms (geomean), with 47% more requests.

### Fixed

- An append whose string or binary column held more than 2 GiB failed with "offset overflow" (one hour of a 400,000-tag
  plant): the rows were concatenated into one batch to sort. Appends now sort groups of up to 1 GiB and merge them.
- The block cache fetched the runs of blocks one read needs one after another; it fetches them at once. First
  queries on object storage with 20 ms per request: TSBS IoT geomean 162 → 125 ms, one truck's history 233 → 109 ms.
- Tigris is recognised when its endpoint comes from `AWS_ENDPOINT_URL_S3` (as Fly.io and the Tigris CLI set it), so
  readers check for new versions with a LIST there, about 245 ms faster per check than a HEAD (#102).

## 0.6.0 – 2026-10-17

### Added

- **Writes from JavaScript.** `TableWriter` in `@timelake/client` appends to tables from Cloudflare Workers, Node,
  Deno, Bun, or browsers, through a `Store` (`r2Store` for an R2 binding, `memoryStore`, or your own). The
  WebAssembly module runs the engine's write path: the same row preparation, Parquet files, commits with retries, last-value
  index and head. Rows are objects or an Arrow IPC stream; `batchId` makes retries safe. Appends run inline
  maintenance like the engine's (size-tier and cold-partition compaction by streaming merge, within `maxCompactBytes`,
  default 64 MiB; retention; snapshot expiry), and `writer.maintain()` runs a full pass. Tables are created with
  the CLI or Python (SPEC §21.10). In Cloudflare Workers, import from `@timelake/client/workers` and pass the
  module from `@timelake/client/timelake.wasm` to `initWasm`. Measured in workerd: an append of 100,000 rows takes
  0.7 s and 37 MiB, compaction of a day of 24 files 2.7 s and 95 MiB (`bench/results/WORKERS.md`).
- **`data-page-rows`**: rows per Parquet data page in the files a table gets (default 20,000; 8,192 for files
  written from JavaScript, where compaction holds a page per column of each file it merges).
- **Version check interval.** The setting `version-check-interval` (table or session) says how long the Timelake
  reader uses a table version it loaded or checked before checking for a newer one. Within it, queries send no
  request for the table's version, and can miss commits by other processes; the process's own commits are seen at
  once, and writers always check. The default, 1 second, is the old fixed behaviour; `0s` checks on every query
  (`SET timelake.version_check_interval = '30s'`, `--set`, `Lake(settings=…)`, or `timelake alter --set`).
- **Ingestion guides.** The [Quick start](https://timelake.dev/docs/quick-start) has eight sub-pages that set up
  continuous ingestion, each with several designs, a recommendation, scheduled checks on recent data, and
  maintenance: Local, AWS (S3, Kinesis, SQS, IoT Core, and Firehose into Lambda), Azure (Blob Storage, Event Hubs,
  Service Bus, and IoT Hub into Azure Functions), Google Cloud (Cloud Storage and Pub/Sub into Cloud Run functions),
  Cloudflare (R2 and Queues, appended by a consumer Worker with `TableWriter` or by a writer script), Fly.io, Vercel,
  and Tigris. Each platform has an
  overview, a setup page, a page per design, and a checks page; the docs sidebar opens and closes nested pages. The
  Quick start compares the platforms and designs. The AWS page compares S3 with S3 Tables.

### Changed

- **Compaction streams.** Compaction merges its input files as sorted streams instead of loading a partition into
  memory, so its memory no longer grows with the partition. Full compaction of TSBS IoT (72 hourly files into 3 days):
  3.65 → 2.37 s, peak memory 1.95 → 0.53 GB, with identical files.
- **Columns are encoded in parallel.** Timelake writes Parquet with its own writer (shared with the WebAssembly
  writer), encoding each row group's columns on threads of their own. The files are the same, byte for byte. The
  compaction above takes 1.17 s instead of 2.37 s; importing 11.7M rows 4.7 s instead of 6.0 s.

### Fixed

- Docs: the storage page's check for conditional writes created a table twice, which fails the second time on any
  store. It now sends a put-if-absent twice with the AWS CLI. The quick start no longer says that Timelake reads AWS
  profiles: it reads the `AWS_*` environment variables (`aws configure export-credentials` exports a profile's). The
  storage page says to create a Tigris lake bucket as single-region or multi-region: on Tigris's default `global`
  buckets, conditional writes are checked within one region only.
- `@timelake/client` didn't run in Cloudflare Workers: Workers can't compile WebAssembly from bytes at runtime,
  which the client did on first use. The `@timelake/client/workers` entry takes the compiled module instead.

## 0.5.0 – 2026-10-17

### Added

- **Dedup on read.** In tables with dedup on, the Timelake reader (CLI, Python, Rust) returns only the latest write of
  each `(series, time)`, before compaction has removed the older copies. Only files that may share a key are merged;
  the rest are read as before. DuckDB, other Iceberg readers and the TypeScript client still see the copies until
  compaction. It can be turned off per table (`timelake.read-dedup = false`) or per session
  (`SET timelake.read_dedup = false`, or `--set timelake.read_dedup=false` on the CLI). A second strategy,
  `timelake.read-dedup-strategy = keys-first`, lists the older copies first and leaves them out of a normal scan.
- **Cold partitions compact sooner.** Once newer data is `timelake.compact-cold-after` (default 1 hour) past a
  partition's end, inline maintenance rewrites the partition into sorted, duplicate-free files when it has two small
  files, even if the append didn't touch it.
- **Insertion time.** Time-series tables get an `_ingest_ts` column: each append fills it with the time the append
  started, unless the input brings its own values (an import keeping arrival times). Among rows with the same
  `(series, time)` the latest insertion wins, then the later commit, everywhere: within an append, on read, in
  compaction and in the last-value index. DuckDB can pick the same rows with
  `QUALIFY row_number() OVER (PARTITION BY <series>, <time> ORDER BY _ingest_ts DESC) = 1`. Leave it out with
  `ingest_time=False` (Python) or `--no-ingest-time` (CLI); `timelake info` shows it.
- **Settings.** Every switch is declared in one registry, with its default, the levels it can be set at (creation,
  table, session, call) and the version that added it. `timelake settings` lists them, and with a table name shows
  the table's values; `timelake alter <table> --set read-dedup=false --unset retention` changes a table's settings
  (`Table.set_properties` in Python and Rust). `Lake(..., settings={...})` in Python sets session settings for every
  query, and `SHOW timelake.read_dedup` shows a session's value. Names work with dashes or underscores, with or
  without `timelake.`. The docs page "Table properties" is now "Settings".
- **Rollups.** `CREATE MATERIALIZED ROLLUP iot.readings_1h AS SELECT device, time_bucket(INTERVAL '1 hour', time) AS
  bucket, avg(value), max(value), count(*) FROM iot.readings GROUP BY device, bucket` creates a table of aggregates
  per time bucket and computes it. Maintenance keeps it up to date: inline after appends once its last refresh is
  older than `timelake.rollup-refresh-interval` (default 1 minute), and on every `timelake maintain`;
  `REFRESH MATERIALIZED ROLLUP` refreshes it now and `DROP MATERIALIZED ROLLUP` removes it. A `GROUP BY time_bucket(…)`
  query on the raw table with the same width and grouping reads the buckets the rollup has right from it, and the rest
  (buckets changed since the refresh, partial buckets at the edges of the time range) from the raw table, so the
  answer is the same as without it; `SET timelake.rollup_routing = false` turns that off. First version: one level from
  the raw table, grouped by series-key columns, with `count`, `sum`, `min`, `max`, `avg`, `first_value` and
  `last_value`. `timelake maintain` and `Table.maintain()` report `rollups_refreshed` and `rollup_rows`
  (`--no-rollups`, `rollups=False` to skip). The docs have a new page, "Rollups".
- **Block cache.** Reads of files a table never changes (data files, manifests, manifest lists, last-value index
  files) go through a cache of 1 MiB blocks, in memory by default on object stores and HTTPS (256 MiB), and on local
  disk with `timelake_cache=disk`, shared by every process on the machine (4 GiB). A query still checks for a newer
  table version, so answers are as fresh as before. Storage options `timelake_cache`, `timelake_cache_dir`,
  `timelake_cache_memory_bytes`, `timelake_cache_disk_bytes` and `timelake_cache_block_bytes`; `Lake.cache_stats()`
  in Python and `Storage::cache_stats()` in Rust count hits and misses. On S3, TSBS IoT repeat queries went from
  54 to 16 ms with no requests, and 16 clients from 288 to 398 queries per second.

### Changed

- **Table format 0.3.0** for new tables (the insertion-time column). Timelake 0.4 and older can't open them: upgrade
  every reader and writer first. `timelake upgrade` raises older tables to 0.3.0 but doesn't add `_ingest_ts` (a
  table's columns are fixed at creation for now).
- **Settings are checked.** Creating or altering a table with an unknown `timelake.*` property, a bad value, or a
  setting fixed at creation (`time-column`, `series-key`, `chunk-interval`, `dedup`, `ingest-time-column`) as a
  property is an error; use the table options for those. `timelake.format-version` can no longer be set by hand.
  `SET` of a table-only setting in SQL is an error. `timelake sql --set` with a bad key reports an error instead of
  crashing.
- Rust: `SessionSettings` has `get` and `flag` instead of public fields. `Session::parse` returns
  `timelake::Statement` (DataFusion's statement, or a rollup statement) for `Session::execute`.
  `MaintainOptions` has a `rollups` field and `MaintainReport` has `rollups_refreshed` and `rollup_rows`.
- With dedup on, rows with the same `(series, time)` within one append keep only the last, so the append's `rows`
  counts them once. Files written by earlier versions may still hold such copies until compaction.
- Benchmarks: Timelake's repeat runs on object storage now read from its memory cache, so their times drop from
  this version on; first runs, in a fresh process, are unaffected. `timebench run --reuse` also runs the concurrent
  dashboards, and `TIMEBENCH_TIMELAKE_OPTIONS` passes storage options to Timelake's readers.
- The TypeScript client's WebAssembly reader is about 4% smaller (6.7 MB), built with Rust 1.99.

### Fixed

- **Compaction could let an older write win.** Inline compaction merged files of a similar size even when a file of
  another size was committed between them, and the merged file then counted as newer than that file: where both held
  the same `(series, time)`, the older value won from then on. Compaction now merges only files next to each other in
  commit order, and a merged file keeps the data sequence number of the newest file it replaces.

## 0.4.0 – 2026-10-14

### Added

- **Install from the usual registries:** `pip install timelake`, `cargo install timelake-cli` (or the `timelake`
  library crate), and `npm install @timelake/client`. The CLI also has one-line installers for macOS, Linux and
  Windows and binaries on each [GitHub release](https://github.com/polythings/timelake/releases), with build
  attestations. See the [Quick start](https://timelake.dev/docs/quick-start).

### Changed

- **Table format versions are semantic versions,** separate from Timelake's: tables created now record
  `timelake.format-version = 0.2.0`. Tables marked `2` by Timelake 0.3.0 read as 0.2.0 and need no upgrade; tables
  without the property are 0.1.0. A newer patch format is read and written by older Timelakes, a newer minor format
  (before 1.0) by neither, with a message to upgrade Timelake. See
  [Upgrading](https://timelake.dev/docs/upgrading#table-format). Timelake 0.3.0 can't open tables created from now on:
  upgrade every reader and writer first.

## 0.3.0 – 2026-10-14

### Added

- **Table format version:** tables record `timelake.format-version` (2 for tables created now; tables without it are
  format 1). Timelake reads tables of any older format and refuses to write to a table of a newer format than it
  knows, with a message to upgrade.
- **`timelake upgrade`** (also `Table.upgrade()` in Python and Rust) brings tables to the current format: records the
  format version, builds a missing last-value index, rewrites unsorted or small files and writes the head. Each step is
  a commit, and no snapshot expires for 7 days (`--keep-snapshots`), so **`timelake rollback <table> <snapshot>`**
  (`Table.rollback()`) undoes it. `--dry-run` shows the plan. New table property `timelake.keep-snapshots-until`.

### Fixed

- After a rollback, a batch id from the abandoned snapshots no longer counts as already committed.

### Changed

- **Faster first queries:** after each commit the writer also writes `metadata/timelake-head`, which holds the table
  metadata, the live files, and (while they fit in 1 MiB) the last-value index and the Parquet footers of the newest
  files. A cold query reads it together with a listing of the table's metadata files that confirms it is current, so a
  first `latest` query takes one round trip to storage instead of three, and a first filtered scan two instead of six.
  A process that already has a table only checks for a newer version. Measured from a laptop, first queries take
  245–545 ms instead of 510–1,030 on S3, and 210–520 instead of 760–1,190 on Tigris. The TypeScript client reads the
  head too and checks for a newer version while its query runs (`TableInfo.manifests` is `null` for a table read from
  its head). Other engines see no change. New table properties `timelake.head-format` (`arrow`, `json`, a codec
  registered with `Lake::register_head_codec`, or `none`) and `timelake.head-inline-bytes`; new storage options
  `timelake_version_lookup` (`list` or `hint`: no LIST, one more round trip), `timelake_freshness_check` (`head` or `list`,
  `list` by default on Tigris) and `timelake_warm_connections` (off by default). The requests each operation makes,
  and what they cost, are in the storage docs. If you write a table with an older Timelake after this one, delete its
  `metadata/timelake-head`.

## 0.2.0 – 2026-10-14

Tagged after the fact: the code at this tag still reports version 0.0.1.

### Changed

- **Faster commits:** an append uploads its data files and metadata all at once, then makes the one conditional
  write, so it waits for two round trips to storage instead of five. The version hint is now updated in the
  background; Rust programs should call `timelake::flush()` before exiting (the CLI and the Python package do).

## 0.1.0 – 2026-10-13

The first version. Tagged after the fact: the code at this tag reports version 0.0.1.

### Added

- **Engine** (`timelake` crate): time-series and plain tables stored as Parquet files with Apache Iceberg metadata, on
  S3, GCS, Azure, S3-compatible stores (R2, MinIO, RustFS) or a local disk. Read-only access over HTTPS.
  - Time-series tables are partitioned by `hour`, `day` or `month` and sorted by `(series key, time)`.
  - Every append is one atomic commit. Concurrent writers are safe without a catalog service (put-if-absent on
    `vN.metadata.json`); a conflicting commit is rebased and retried.
  - Idempotent appends with a `batch_id`: a repeated batch is skipped and reported as 0 rows.
  - A last-value index per snapshot gives the newest row of every series without a scan.
  - Maintenance (compaction, deduplication where the latest write wins, retention, snapshot expiry) runs inline after
    appends, or on demand. A maintenance failure is reported separately and never undoes the append.
  - SQL through DataFusion, with `time_bucket()`, file, row group and page pruning, and "latest row" queries rewritten
    to read the index.
- **CLI** (`timelake`): `create`, `append`, `import`, `sql`, `latest`, `maintain`, `info`, `ls`, `duckdb`, `drop` and
  `index`. Loads Parquet, CSV, gzipped CSV and TSV.
- **Python** (`timelake` package): `Lake` and `Table` with `create_table`, `append` (pyarrow, polars or any Arrow
  stream), `sql`, `latest` and `maintain`, returning pyarrow tables. Full type stubs. Invalid input raises
  `ValueError`, a missing table `KeyError`, and an unresolvable concurrent commit `timelake.ConflictError`.
- **WebAssembly reader and TypeScript client** (`@timelake/client`): reads tables over HTTPS in the browser or Node,
  with latest, range and time-bucket queries and Zod-typed rows.
- **Open format:** plain DuckDB, Spark and PyIceberg read every table. `timelake duckdb <table>` prints the SQL.
