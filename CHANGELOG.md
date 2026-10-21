# Changelog

Changes users can see, newest first. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
The crates, the CLI, the Python package, the WASM reader and the TypeScript client share one version, which follows
[Semantic Versioning](https://semver.org) as [Cargo applies it](https://doc.rust-lang.org/cargo/reference/semver.html):
before 1.0, a minor release (0.3 → 0.4) may break things, and its entry says how; a patch release never does. Each
version is a [tag on GitHub](https://github.com/polythings/timelake/tags), so its code and benchmarks can be checked out.

## Unreleased

### Added

- The website has a light theme. It follows the system's setting, and a button in the header switches between light and
  dark.
- `ASOF [LEFT] JOIN r ON l.key = r.key AND l.time >= r.time` (DuckDB's form) and `ASOF JOIN r MATCH_CONDITION
  (l.time >= r.time) ON l.key = r.key` (Snowflake's): each left row with the right row nearest in time. A day of 8.6 M
  IIoT readings matched with machine states: 0.23 s, against 78 s as a range join.
- Aggregates `ohlc(time, price)`, a group's open, high, low and close; `lttb(time, value, n)`, a series downsampled
  to `n` points for a chart (Largest-Triangle-Three-Buckets); and `histogram(x, low, high, bins)`, counts per bin as
  numpy's.
- Aggregates `quantile(x, q)`, exact with pandas' linear interpolation (DataFusion's `percentile_cont` drifts in the
  sixth digit), and `approx_quantile(x, q)`, a name for the t-digest `approx_percentile_cont`.
- Aggregates `flatline_count(time, value, threshold)` and `flatline_seconds(…)`: how often and how long a series kept
  exactly the same value for longer than a threshold (a stuck sensor). A day of 700 IIoT tags: 0.13 s, against 4.4 s
  written with window functions.
- Window functions `diff(x)`, `percent_change(x)` (`pct_change`), `rate(time, x)`, `rolling_mean(x, n)`,
  `rolling_std(x, n)` and `zscore(x)`, as pandas defines them; `rolling_mean` over 60 rows is about 4× faster than a
  framed `avg`. Aggregates `skewness` (`skew`) and `kurtosis` (`kurt`), and the pandas names `std` and `cov`.
- Aggregates `first(time, value)` and `last(time, value)`, a group's value at its earliest and latest time, and
  `value_at(time, value, at)` (alias `asof`), the latest value at or before a time. Rollups answer `first` and `last`.
- Rollup routing follows filters on grouping columns that come from another table, such as `tag IN (SELECT tag FROM
  tags WHERE line = 'L0002')`: the rollup's rows are kept the same way. IIoT daily statistics for one line's tags from an
  hourly rollup: 551 → 37 ms.
- The design moved from one file, `SPEC.md`, to numbered specs in `spec/` (in the spirit of Rust's RFCs and Python's
  PEPs): one spec per feature with the same template, its status (done, in progress, planned, dropped), the releases
  that changed it, and its history, and an index for people and agents. Milestones made of specs (`spec/milestones/`,
  v0 among them) are the roadmap, and background pages hold the purpose, architecture, license and releases. The
  website shows them at [/spec](https://timelake.dev/spec) and [/roadmap](https://timelake.dev/roadmap), generated from
  the same files. Open questions and tasks moved to GitHub issues (#104–#143), linked from the specs.

- Aggregates `integral(time, value [, 'forward_fill' | 'linear'])` (the area under the signal, in value × seconds),
  `counter_diff(time, value)` (how much a counter grew; a drop is a reset to zero) and `counter_rate(time, value)` (that
  growth per second), exact over irregular readings like `time_weighted_mean`, and about as fast (hourly values of
  1,000 tags over 30 days: 2.2 s, `avg` 2.0 s). Rollups answer them from a stored `time_weighted_mean`, or store them.
- The signal aggregates take a condition as the value, 1 while true: `integral(time, pressure > 8.5)` is the seconds
  above 8.5, `time_weighted_mean(time, state = 'running')` the share of time running. IIoT `time-above-limit` written
  this way: 334 → 191 ms, same answers.
- Gap filling: the table function `time_buckets(width, start, end [, origin])`, every bucket of a range to join
  readings against, and the window functions `forward_fill` (`ffill`), `backward_fill` (`bfill`) and
  `interpolate(x [, t])`. `forward_fill` over 10 M rows takes 0.26 s; `last_value(x) IGNORE NULLS` 3.6 s.
- Aggregates `gap_count(time, threshold)` and `gap_seconds(time, threshold)`: how many times a series' readings
  stopped for longer than `threshold` (an interval, such as `INTERVAL '5 minutes'`), and how long those gaps lasted in
  all, exact over each group's first to last reading. The gaps of 100 one-second tags: a day 226 → 77 ms, 30 days
  17.8 → 1.4 s against the same query written with `lead()`.
- Rollups answer queries over a time range without a `time_bucket`, grouped by some of their columns (each tag's
  average over the last day): the rollup's buckets inside the range, the raw rows at its edges, merged. Averages need
  the rollup's `sum` and `count` of the column. With an hourly rollup on the industrial benchmark, 30 days of
  statistics per tag take 55 ms instead of 1.75 s, of time-weighted means 0.15 s instead of 1.5 s.
- Rollups store `gap_count` and `gap_seconds` (as their state for the threshold) and answer both from it; 30 days of
  gaps per tag from an hourly rollup: 44 ms instead of 1.34 s.
- Setting `window-series-order` (`auto`, `true`, `false`): a window over each series in time order
  (`lead(…) OVER (PARTITION BY <series key> ORDER BY time)`) reads the files merged in series order, so they are
  neither redistributed nor sorted. `auto` does so when the files are in one time partition: IIoT `gaps` 260 → 215 ms,
  `time-above-limit` 396 → 327 ms. Over a week of files merging is slower (2.0 → 3.0 s), so `auto` leaves it.
- Setting `shared-signal-state` (on by default): signal aggregates of the same column in one query compute their runs
  of readings once. All four, hourly for 1,000 tags over 30 days: 3.5 → 2.1 s.

### Fixed

- A table with the property `read-dedup = false` was still answered from its rollups, which count each reading once,
  so its `GROUP BY time_bucket(…)` queries could differ from reading the table. Only a session's
  `SET timelake.read_dedup = false` stopped routing; the table's property now does too.
- `SET timelake.rollup_routing = true` didn't route a table whose `rollup-routing` property is off. The session's value
  now overrides the table's, as for every other setting.
- Snapshot expiry from the TypeScript writer retried at once and without end while other writers kept committing first.
  It waits between attempts and gives up after 10, as commits do.
- Snapshot expiry in the engine stopped with an error when a manifest only expired snapshots used was already gone (an
  earlier expiry was interrupted). It skips it now, as the TypeScript writer did; a missing manifest of a snapshot it
  keeps still stops it.
- The TypeScript client and `TableWriter` let every manifest entry without a sequence number take its manifest's,
  where Iceberg gives it only to files the manifest's snapshot added. Such an entry from another writer now counts as
  the oldest data, as in the engine, so of two rows with the same key and time the newer write wins in both.
- Two appends at once through one TypeScript `TableWriter`, or one Python `Table` from two threads, could leave the
  handle on the older of the two versions when they finished out of order. A handle now only moves forward.
- The TypeScript client's `latest` on a table without a last-value index could return an older write of a reading
  rewritten at the same time: it read files newest first, and the last one read won. Files are now planned oldest data
  first, so the newer write wins, as in the engine.
- The TypeScript client failed to return rows or latest values from files with different columns (written before a
  column was added, or by another writer), or with a column at a narrower type (`int` before it became `long`). Rows
  are now matched by column name, with nulls where a file lacks a column.
- The TypeScript client ignored a series filter or time column the table doesn't have (a typo such as `tags_Id`) and
  returned every row; such a query is now an error naming the column. A file written before a filtered column was
  added has no rows matching the filter, rather than all of them.
- A time column whose name isn't a valid Avro name, such as `event-time`, gave the table a partition field
  (`event-time_day`) that manifests can't hold: reading them without the head failed, and the TypeScript client
  couldn't commit at all. New tables name it as Iceberg's Avro encoding does (`event_x2Dtime_day`), and the TypeScript
  client writes any partition name, with the field id the partition spec gives it. Existing tables with such a
  column need recreating.
- On a table with delete files (row-level deletes another engine such as Spark or Trino wrote), the TypeScript
  client's reads skipped them, so deleted rows came back, and its commits wrote their manifests back as data
  manifests. Both are now an error naming the delete file.
- Snapshot expiry removed branches and tags that another engine had made on old snapshots, and deleted the files only
  they used. A snapshot a branch or tag points at is now never expired.
- A `max-snapshots` or `min-snapshots` near 2^64 overflowed the check for inline snapshot expiry after an append, and
  a `target-file-size-bytes` near 2^63 the compaction plan: a crash in debug builds, a wrong decision in release ones.
- Data files recorded the bounds of a decimal column wider than 18 digits as 16 padded bytes; the Iceberg spec asks
  for the fewest bytes that hold the value, as Timelake already wrote narrower decimals.
- A `write.parquet.compression-level` that isn't a level of the codec (`high`, or a negative level for `gzip` or
  `brotli`) was ignored, and files were written at the default level. Setting one is now an error, and writing to a
  table that has one fails, naming the property.
- A setting given twice in one call under its two spellings (`timelake.read-dedup` and `timelake.read_dedup`) kept
  either value, at random. It is now an error.
- `timelake.row-group-rows=0` was accepted and then crashed every write; `timelake.target-file-size-bytes` and
  `timelake.data-page-rows` took 0 too. All three must now be positive.
- The WebAssembly reader compared the row-group statistics of a decimal column, stored unscaled (1.5 as 15), with a
  query's values, so a filter on `1.5` skipped the rows holding it. Statistics of decimals, unsigned integers and other
  types it can't compare are no longer used to skip row groups.
- The WebAssembly reader read the file bounds of a `timestamp_ns` column in a table's head as microseconds, so a
  time range skipped files that held matching rows. It scales them as it does a manifest's.
- Compaction from the TypeScript client dropped the table's last-value index: later appends didn't rebuild it, so
  `latest` queries read every file from then on. Compaction and retention now keep it, as the engine's do.
- Appending nanosecond timestamps to a microsecond column (or a `date64` with a time of day to a date column)
  truncated them, so two distinct times could become one row. Converting to a coarser unit is now checked like other
  conversions: a value it would change is an error. A supplied `_ingest_ts` that doesn't parse is an error too; it was
  replaced with the time of the append.
- The CLI prints a failed query's error once: DataFusion's errors repeat their cause, and each was printed again.
- `time_weighted_mean` over a span of several files per scan partition, such as a month of daily files grouped by
  tag, failed with "runs that overlap in time": each partition now reads a stretch of time, kept to its files. Files
  that need merging are merged with the files of the time they span, not with the whole table.
- The TypeScript client's `TableWriter` trims trailing slashes from the warehouse in linear time; the regular
  expression it used took quadratic time on a string with many slashes.
- The TypeScript client's `TableWriter` writes `bigint` values beyond 2^53 exactly; it rounded them to the nearest
  float. Where `JSON.rawJSON` is missing (runtimes before ES2025), such a value is an error.
- Settings given at creation by their short names (`--prop retention=90d`, `properties={"read-dedup": "false"}`) were
  stored under that name and ignored. They are now read as `alter` reads them, and an unknown setting is an error.
- `timelake info` showed a retention shorter than a day as `0d`. It shows the duration as set (`12h`), and the JSON's
  `retention_days` is now `retention`, the same string.
- Python: `Lake(storage_options=…, settings=…)`, `create_table(properties=…)` and `set_properties` take any mapping, as
  the type stubs say; they raised `TypeError` for anything but a `dict`.
- The WebAssembly module grew every vector and string by copying it into a new block: its memory-counting allocator
  left out `realloc`, so the system allocator's growth in place went unused.
- The WebAssembly reader (the TypeScript client) treated every time column as microseconds: on a `timestamp_ns` column,
  or a Parquet file with millisecond times, time ranges, buckets and pruning gave wrong results without an error.
- A failed write from the TypeScript client (or its compaction) left the multipart upload it had started open, and
  the files it had already finished in the table's directory; S3 and R2 keep and bill such parts. It now aborts the
  upload and deletes the files.
- A commit whose metadata write landed but reported a failure (a retried request that got "already exists", or a
  timeout) was taken as lost or failed, and its cleanup deleted the manifests (and for an append, the data files)
  the committed version uses, leaving the table pointing at missing files. The writer now reads the version back: its
  own metadata there is the commit. When that can't be told, the commit fails with "commit outcome unknown" and
  deletes nothing. Timelake also no longer commits metadata it couldn't load itself.
- Snapshot expiry treated a kept snapshot whose manifest list was missing as using no files, and deleted every file it
  shared with the expired snapshots. It now reads what the kept snapshots use before changing anything, and stops with
  an error that names the broken snapshot.
- Benchmarks: ingest request counts and costs cover only the writes; they included the checkpoint queries and the
  live probes, which now go through an uncounted proxy listener (`-listen-uncounted`). Ingest seconds no longer
  include deleting the previous run's tables, and every S3 run sets and records the proxy's latency and throughput
  cap instead of keeping the last run's.
- Benchmarks: concurrent dashboard clients start together once all have opened their readers, so queries per second
  counts clients that overlap. A/B reader settings apply to probes and dashboard clients too, not only the measured
  queries.
- Benchmarks: ClickHouse's answer to TSBS IoT `avg-daily-driving-session` is right; the query relied on `lead` giving
  NULL at a series' edge, which ClickHouse doesn't. QuestDB commits reuse one client instead of connecting each time.
  S3 Tables maintenance that never compacted is an error, not ok. Lance's maintenance on the local S3 target goes
  through the counting proxy like every other system's (`BENCH_LANCE_DIRECT`, which sent it around the proxy, is gone).
- With read deduplication off, `ORDER BY` the series key and time could return the rows of a file another engine wrote
  unsorted in the file's order. Such files are now sorted.
- A `column IN (…)` list of more than 20 values with `NaN` among them could skip the parts of a file holding `NaN`
  rows. Such lists no longer prune.
- `time_buckets`, `gap_count`, `gap_seconds` and the flatline aggregates overflowed (a panic in debug builds, wrong
  buckets otherwise) on intervals or timestamps beyond the microsecond range, such as `INTERVAL '200000000 days'`.
  They are now an error.
- With `SET timelake.rollup_routing = false`, planning a query still loaded the table's rollups, a few requests each,
  and so did every `CREATE MATERIALIZED ROLLUP`. They no longer do.
- `CREATE EXTERNAL TABLE IF NOT EXISTS` naming an existing lake table failed instead of doing nothing, and the
  DataFusion context's `table_exist` was false for tables until an `information_schema` query had listed the lake. It
  now also knows the tables the process has created or read.
- A latest-row query written `WHERE code = 1 AND rn = 1` (another column compared with 1 before `rn`) read every row
  instead of the last-value index.
- With `SET timelake.filter_pushdown = false`, a newest-first query (`ORDER BY time DESC LIMIT k`, or the newest `k`
  buckets) with a filter that file statistics can't rule out returned too few rows, often none: it stopped after the
  newest partition's unfiltered rows. Each partition's rows are now filtered before they count.
- A failed append or compaction could leave multipart uploads open on object storage, billed until a lifecycle rule
  removes them: the upload of the file being written, those of the other partitions' files, and uploads cut off when
  another one failed. They are now aborted, or run to their end and deleted. (A part that fails only once the file is
  finished can still leave its upload open: object_store gives no way to abort it.)
- A stray file in the disk cache's directory (such as macOS's `.DS_Store`), or an object directory removed while a
  sweep ran, stopped every sweep, so the disk cache grew past its budget. Sweeps now pass over such entries.
- A read cut short (a `LIMIT`, a timeout) could leave the block cache's GETs half done: the cache, and the store under
  it, then stayed in memory for good, and a later read resumed the stale request. Each fetch now runs to its end on
  its own, and another read can't miss both the fetch and its blocks.
- Through the block cache, a missing object was a generic error, so snapshot expiry failed instead of passing over a
  manifest list another process had already deleted. It is `NotFound` again.
- Changing settings, `rollback` and snapshot expiry retried without limit or pause when another writer kept publishing
  first (or a store kept reporting the version as taken). They now back off and give up with a conflict error after 10
  attempts, as commits do.
- A table with `head-format = none` sent a DELETE of its head after every commit. Each process now deletes it once.
- The disk cache keyed S3 objects by the endpoint only when it was given in a few option names, not when it came from
  `AWS_ENDPOINT_URL`/`AWS_ENDPOINT_URL_S3` or as `endpoint_url`: two endpoints with the same bucket name could then
  share cached blocks. The endpoint now comes from the same place as the store's own.
- The engine (and so the CLI and Python) read the delete files of a table another engine had written row-level deletes
  to as data, so deleted rows came back, and its commits and compaction would carry them on. Reads and writes of such
  a table are now an error naming the delete file, as in the TypeScript client.
- `timelake import` and `append` failed on a file whose extension isn't lower case (`data.CSV`). Any case works.
- `timelake sql` printed nothing for an empty result with `--format csv` and `++` as a table; both now show the
  result's columns. A statement without columns (`SET`) prints nothing.
- The CLI's error messages could leave out a cause whose text appeared inside an earlier message. Only a cause the
  message before it ends with (DataFusion's repeats) is left out.
- `timelake sql --set read_dedup=false` failed: the name needed its `timelake.` prefix. A name without a prefix is now
  a Timelake setting, as in Python.
- `timelake info --json` gives `format_version` as a number (`2`, as the TypeScript client's `info()` does), not
  `"V2"`, and the text output shows snapshot times in UTC, marked `Z`. `timelake maintain --help` describes
  `--no-compact`, `--no-retention` and `--no-expire`.
- `--chunk` and `--retention` of `timelake create` and `import` are checked when the arguments are parsed, with the
  usage message, before any file is read.
- Python: `repr(Lake(...))` quotes the location as Python's `repr` does (`Lake('…')`), not with Rust's double quotes.
- TypeScript client: a table handle many commits behind reads the newest version. It moved forward at most two
  versions per query and took that for current; it now probes forward (doubling, then halving the step) until a
  version is missing.
- TypeScript client: a handle behind the metadata files maintenance deleted reads the newest version. It kept
  answering from its old version.
- TypeScript client: public S3 and CloudFront buckets that answer `403` for a missing key work. Every query after
  `cacheMs` failed on the check for a newer version.
- TypeScript client: a query whose request fails frees the Parquet files it already opened.
- TypeScript client: `lake.tables()` checks that `timelake-tables.json` is a list of names.
- TypeScript client: `zodSource` names a table such as `status` `Status`, not `Statu`.
- `scripts/serve-range.mjs` answers a malformed path with 400 instead of crashing, ends a response whose file can't
  be read, serves the whole file for a suffix range longer than it (`bytes=-500` of 100 bytes), and ignores
  `bytes=-`. The website's lab route reads suffix ranges too.
- The website's benchmark pages show sizes in decimal units throughout: a megabyte is 1,000,000 bytes, as the labels
  say. The per-system storage column divided by 1,024, so it showed the same bytes as about 5% fewer MB than elsewhere.
- The website shows a time just under a second as "1.00 s" instead of "1000 ms", and likewise at the other unit
  boundaries.
- The website draws a system it has no color for in gray, listed after the known ones, instead of in S3 Tables' color
  and first.
- On the website's benchmark pages, closing a query's results on a narrow screen and then widening the window no longer
  leaves the results pane empty, and a ranking whose values are all zero draws its bars instead of failing.
- Resizing the website's window redraws the requests chart's dots instead of stretching them.
- Benchmarks: maintenance request counts and costs cover only the maintenance itself; they included reading the
  tables' state and sizes afterwards (all metadata for Iceberg, a listing for Timelake, manifests for S3 Tables).
  Saving the state for the live probes is no longer counted as ingest either.
- Benchmarks: ingest seconds leave out reading the workload's data from its files (the initial rows and each
  arrival). With `--pace`, the next arrival is read while waiting for its time, and there is no wait after the last
  one.
- Benchmarks: a `--reuse` run starts each database server and records its versions, and skips a query at later
  checkpoints once it timed out, as a full run does.
- Benchmarks: `timebench report` shows only the costs the run priced; it showed S3 Standard storage prices for
  database servers, S3 Tables and the reference.
- Benchmarks: each TSBS IoT arrival holds the same time window of readings and diagnostics; they were paired by
  position, so a window missing from one table shifted the rest.
- Benchmarks: concurrent dashboards' queries per second count the queries that finished within the measured seconds,
  not the last one each client started before the end.
- Benchmarks: answer checks sum `Date64` columns as the Timelake CLI does, plain Parquet files are split into days
  correctly for any timestamp unit, and QuestDB maintenance closes the connection ingest left open.
- Benchmarks: the S3 proxy in front of a local object store counts a PUT with `Expect: 100-continue` once and adds
  its latency once; the interim `100 Continue` response made it count the request twice.
- Benchmarks: a system missing a setting (such as `BENCH_TIMESCALEDB_DSN` or S3 Tables' `BENCH_TABLE_BUCKET_ARN`)
  fails its own ingest and the run goes on; it stopped the whole profile and left the run file without its end. A
  database server is stopped even when reading its version fails.

### Changed

- Maintenance in the engine merges several groups of files at once (up to 8, reading at most 24 files together, the
  most one merge reads), instead of one partition after another. Rollup refreshes load their rollups, and queries
  look up the rollups they may route to, at once rather than one by one.
- Maintenance in the engine (`timelake maintain`, Python's `maintain()`, Rust) merges at most 24 files at once, as the
  TypeScript writer does, so a merge's memory stays bounded. A partition with more files to merge is merged in parts,
  the report's `more_to_compact` says so, and the next run finishes it (`timelake upgrade` runs until it is done). A
  table now compacts to the same files whichever writer runs maintenance.
- Maintenance reports have the same fields everywhere. Rust's `MaintainReport` gains `bytes_rewritten` and
  `more_to_compact`; Python's dict gains those and `files_dropped`; the TypeScript `MaintainReport` gains
  `rollupsRefreshed`, `rollupRows` (always 0 there: only the engine refreshes rollups) and `seconds`.
  `partitions_compacted` counts each partition once, not each merged group. `timelake maintain` also prints the files
  retention dropped, the megabytes compaction read, and when to run again.
- Rust: `MaintainOptions` has a `max_bytes` field in the engine too, the bytes one compaction run reads at most, and
  `MaintainSteps` builds options from the steps to run. `MaintainOptions`, `MaintainReport` and `RollupRefresh` are
  `timelake_format`'s, re-exported from `timelake` as before.
- Snapshot expiry lists only the table's metadata files (`metadata/v…`) to find old versions to delete, not the whole
  metadata directory with every manifest.
- Appends convert a column to the table's type only where no value is lost. A value that doesn't fit (an integer out
  of range, text that doesn't parse, a fraction for an integer column, an integer above 2^53 for a double) is an error
  naming the column; it used to be written as NULL or truncated without a word. Conversions that are never exact, such
  as a boolean or a timestamp to an integer, are an error too. The architecture page lists what converts.
- The website's Spec section opens on the first spec, with every spec in the sidebar (and in the menu on phones),
  instead of a list page. Spec pages are wider, and table cells break long words only when they don't fit.
- The website's roadmap is a board with a column per status and a small card per milestone; a milestone's page has no
  sidebar. The benchmarks page lists the benchmarks in a sidebar and shows each one's query times after maintenance,
  with the rest on its own page. The requests chart is drawn complete instead of played. The home page no longer has
  a benchmarks section. Query cost is shown for one query (`$0.000021`) instead of for a million. The header's icons are evenly spaced, and with a
  mouse or trackpad the page no longer bounces past its ends, which dragged the header along. Scrolling is smoother:
  nothing reacts to the cursor while the page scrolls (hover effects, chart tooltips and legends, the hero), the
  use-case pictures read their colors once instead of every frame, and the header is solid instead of blurred.
- Setting `newest-first` also covers the newest buckets of a `GROUP BY time_bucket(…)` sorted by the bucket with a
  limit: older time partitions are skipped once they can't hold a row of those buckets. TSBS DevOps
  `groupby-orderby-limit`: 254 → 115 ms.
- Appends to tables with many series extend the last-value index by merging it with the append's newest rows instead
  of sorting both: a 100,000-row append to a table of 1,000,000 series takes 171 ms instead of 264 ms.
- When several rollups could answer a query, the widest is used. A rollup answers time-weighted averages, integrals
  and counters only where it holds 32 readings or more per row, `m4` and gaps at 4: daily time-weighted means from a
  1-minute rollup of one-second readings took 3.6 s, against 2.0 s from the readings, and now read the readings.
- **Breaking:** default names of rollup columns use `_` where they had `:` (`integral_linear_value`, not
  `integral:linear_value`). Existing rollups keep their names.
- Float columns are written with a dictionary, falling back to byte-stream-split once it outgrows its page, instead
  of byte-stream-split alone (setting `float-encoding`). IIoT readings take a third of the space (5 days: 366 → 119
  MB) and TSBS DevOps scans run up to twice as fast (`high-cpu-all` 274 → 136 ms, 38% more space); random doubles cost
  5% more space and 6% more scan time. Files already written keep their encoding until compaction rewrites them.
- **Breaking:** setting `time-weighted-mean` is now `series-order` (SQL `timelake.series_order`), since it applies to
  all four aggregates over a signal. The state a rollup stores for `time_weighted_mean` gained fields: a rollup with
  `time_weighted_mean` created by 0.7.0 is no longer used to answer queries, and its refresh fails until it is dropped
  and created again.
- Planning a newest-first query (`ORDER BY time DESC LIMIT k`) reads the file footers of all time partitions at once
  instead of one partition after another: one round trip instead of one per partition, when the footers aren't in the
  table's head.
- TypeScript client: durations use the engine's syntax, a whole number and `s`, `m`, `h`, `d`, `w` or `y`, or spelled
  out (`90 minutes`, `1 day`). `500ms` and fractions such as `1.5h` are now errors; pass a number of milliseconds
  instead (`500`).
- TypeScript client: time strings must be ISO 8601, and one without an offset (`2026-01-03T12:00:00`) is UTC, as in
  the engine's SQL. It was local time, so the same query returned different rows in a browser and on a server.
  Strings keep microseconds.
- TypeScript client: `table.lastStats` counts the requests that loaded or checked the table version for the query
  (the head, the version hint, metadata and manifests), and `bytes` is what was received, whole files included when a
  server ignores `Range`.
- Rust API: `timelake::align_batch` and `timelake::latest_per_series` are `timelake-format`'s and return its
  `WriteError`, which `?` turns into a `timelake::Error`. `TableName` is `timelake-format`'s too (still re-exported),
  so a bad table name is an `Error::Format`, not an `Error::Invalid`.
- A table property outside its setting's values (`read-dedup-strategy`, `filter-pushdown`, `series-order`,
  `window-series-order`, `float-encoding`), which only another engine could have written, makes opening the table an
  error naming the property, as a bad boolean or duration already did. `series-order` used to fall back to `auto`,
  `float-encoding` to `dictionary`, and the others failed queries.
- Table creation and `timelake alter` (`Table.set_properties` in Python and Rust) reject an unknown
  `write.parquet.compression-codec`, or a `write.parquet.compression-level` the codec doesn't have, instead of letting
  the next write fail. Set by another engine, such a value still fails writes only; the error is now a `ValueError` in
  Python, where it was a `RuntimeError`.
- Rust API: the contract's settings with fixed values are enums in `timelake_format::value` (`ReadDedupStrategy`,
  `AutoBool`, `SeriesOrder`, `FloatEncoding`, `CompressionCodec`), `Contract::compression` holds the checked codec and
  level, `Contract::head_format` is `None` for no head, and `Setting::kind` is a `settings::Kind`.
- Rust API: `timelake_format::rollup` holds a rollup's aggregates as `timelake.rollup-aggregates` stores them, each
  function a `RollupFunction` (the stored JSON is unchanged), and the settings registry checks that property against
  it.
- Rust API: `Table::files` returns the live files as a shared `Arc<Vec<LiveEntry>>` instead of a copy.
- Rust API: `RollupStatement`'s names are DataFusion `TableReference`s, parsed once, so a quoted name keeps its dots.
- Rust API: `timelake_format::IcebergType` names a column's Iceberg type: `bounds::decode` takes one and
  `metadata::Column::iceberg_type` is one (`Nested` for structs, lists and maps). `NewSnapshot::operation` is an
  `Operation`, which moved to `timelake_format::metadata` (still re-exported from `commit`).
- Rust API: `timelake_format::merge::Merge::new` takes a `MergeOptions` (inputs, `unique`, `batch_rows`) instead of
  three positional numbers, and `iceberg::write_manifest_file` writes a manifest and returns its manifest-list entry,
  replacing `manifest_file`.
- Rust API: `timelake_format::FormatError` is an enum (`FormatError::Invalid` holds a message; Arrow and JSON errors
  of the head keep their cause), and `WriteError` reports its cause through `source()`, with Avro errors as
  `WriteError::Avro`. An append's `maintenance` error is the `WriteError` itself, not its message. Arrow errors are no
  longer prefixed with `head:`.
- TypeScript client: `TableWriter` takes a table name without a namespace (`readings` is `default.readings`), like the
  reader and the engine, and both check names with the engine's rules, so `iot/readings` or `a.b.c` is an error.

### Removed

- The website's Explorer (`/explore`, with `/api/sql` and `/api/series`): the browser for tables, latest values,
  series charts and SQL is developed in its own project.

## 0.7.0 – 2026-10-18

### Added

- Aggregate `time_weighted_mean(time, value [, 'forward_fill' | 'linear'])`: the mean of irregular readings weighted
  by how long each held, over each group's first to last reading; exact, as fast as `avg` (hourly means of 1,000 tags
  over 30 days: 2.0 s). Rollups store its state and answer wider buckets. Setting `time-weighted-mean` (`auto`,
  `segments`, `merge`) for tables with dedup off whose files overlap.
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

- **Breaking:** Timelake's scans no longer follow DataFusion's `datafusion.execution.parquet.pushdown_filters`; set
  `timelake.filter_pushdown` instead (see Added).
- **Breaking (Rust):** in `timelake-format`, `rows::PartitionRows` holds a partition's rows as a list of batches
  (`(Option<i32>, Vec<RecordBatch>)`), and `Contract` has new fields for the new settings. Callers of `sort_and_split`
  iterate the batches; code that builds a `Contract` field by field uses `Contract::from_properties`.
- Tables created by other engines with a codec other than ZSTD in `write.parquet.compression-codec` are now written with
  that codec (see Added); tables Timelake created already say ZSTD.
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
  the CLI or Python ([spec 13](spec/0013-wasm-writes.md)). In Cloudflare Workers, import from `@timelake/client/workers` and pass the
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
