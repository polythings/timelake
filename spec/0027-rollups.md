---
number: 27
title: Materialized rollups
area: Reading
status: done
updated: 2026-10-20
versions: [0.5.0, 0.7.0, unreleased]
related: [12, 28, 30, 31]
issues: [15, 108, 136]
---

# 27. Materialized rollups

**Done** · updated 2026-10-20 · versions 0.5.0, 0.7.0, unreleased · milestones: [Time-series SQL](milestones/time-series-sql.md), [Millions of series](milestones/millions-of-series.md)
· related: [spec 12](0012-late-data-and-dedup.md), [spec 28](0028-function-library.md), [spec 30](0030-signal-aggregates.md), [spec 31](0031-m4.md)

## Summary

A rollup is a Timelake table of aggregates per time bucket, defined in SQL, refreshed incrementally from the raw table's changed buckets, and used automatically by the reader with exact answers. Coarser queries and fewer keys are answered by re-aggregating a rollup, and wider rollups are computed from narrower ones.

## Design

### Materialized rollups
A materialized rollup is a table of aggregates per time bucket (for example, per device per minute) that Timelake
keeps up to date as data lands, so dashboards read a small table instead of the raw data.
- Defined declaratively in SQL:
  ```sql
  CREATE MATERIALIZED ROLLUP sensor_1m AS
  SELECT device_id, metric,
         time_bucket('1 minute', ts) AS bucket,
         avg(value), min(value), max(value),
         time_weighted_mean(ts, value, 'linear') AS twa,
         count(*)
  FROM iot.sensor
  GROUP BY 1, 2, 3
  WITH (refresh_lag = '2 minutes', retention = '2 years');
  ```
- **Every rollup is itself an Iceberg table**, so DuckDB and others can query rollups directly.
- **Incremental refresh:** Only buckets touched since the last refresh are recomputed (from snapshot diffs). This handles late data correctly.
- **Hierarchical rollups:** 1 min → 1 h → 1 d, each built from the previous level.
- **Real-time aggregation (Timelake reader):** Combines materialized buckets with on-the-fly aggregation of the not-yet-materialized tail and hot-tier data.
- **Automatic query routing:** The Timelake reader rewrites `GROUP BY time_bucket(...)` queries to use the best matching rollup.
- **Mergeable state:** Aggregates that can't be re-merged from final values (percentiles, time-weighted averages) store intermediate **state** (sketches, partial sums).

#### First version (built 2026-10-16)
The first version builds one level, from the raw table, with aggregates that a bucket's rows give exactly. What it
leaves out is listed at the end.

**Definition.**
```sql
CREATE MATERIALIZED ROLLUP [IF NOT EXISTS] iot.readings_1m AS
SELECT device, time_bucket(INTERVAL '1 minute', time) AS bucket,
       avg(value) AS avg_value, min(value) AS min_value, max(value) AS max_value, count(*) AS n
FROM iot.readings
GROUP BY device, bucket
[WITH (retention = '2y', chunk_interval = 'month', refresh_interval = '5m')];

REFRESH MATERIALIZED ROLLUP iot.readings_1m;   -- bring it up to date now
DROP MATERIALIZED ROLLUP [IF EXISTS] iot.readings_1m;
```
- The query reads one Timelake time-series table, with no `WHERE`, join, or subquery. It groups by one
  `time_bucket` (or `date_bin`) of the table's time column, with a fixed width (no months) and the default origin, and
  by any of the table's series-key columns, each of which must be selected.
- Aggregates: `count(*)`, `count(x)`, `sum`, `min`, `max`, `avg`, and `first_value` / `last_value` ordered by the
  time column. Unaliased outputs get names like `avg_value` and `bucket`.
- Grouping only by series-key columns keeps a row in the same group when a newer write of it replaces it ([spec 12](0012-late-data-and-dedup.md)), so
  appending recomputed buckets is enough: nothing ever has to be taken out of a rollup.
- `WITH` takes the rollup table's `retention`, `chunk_interval` (default: day for buckets under an hour, else month)
  and `refresh_interval`, and any other table setting.

**Storage.** A rollup is an ordinary Timelake table: time column `bucket`, series key the grouping columns, dedup on,
no insertion-time column. Its properties hold the definition, which Timelake manages:
`timelake.rollup-source` (the raw table), `timelake.rollup-bucket` (the width) and `timelake.rollup-aggregates`
(JSON: output name, function, column). The raw table lists its rollups in `timelake.rollups`. Every snapshot of the
rollup records in its summary the raw table snapshot it reflects: `timelake.rollup-source-snapshot` and
`timelake.rollup-source-sequence` (that snapshot's sequence number), carried forward by the rollup's own compaction.
No format change: older Timelake versions read a rollup as a plain table and don't refresh it.

**Refresh.** The changed buckets are those covered by the time bounds of the raw table's live data files with a data
sequence number above the recorded one. Compaction keeps the newest data sequence number of the files it merges
([spec 19](0019-compaction.md)), so rewritten files whose rows the rollup already reflects don't count as changed; retention's deletes don't
either, so a rollup keeps history the raw table has dropped. The changed buckets are recomputed from the raw table
(read with dedup on), a window of at least a day at a time, and appended to the rollup, about a million rows per
commit, the last commit recording the new raw snapshot. Dedup on read keeps the newest copy of each bucket, and the
rollup's own compaction removes the rest. A refresh that fails halfway leaves the old record in place, so the next
one recomputes the same buckets.
- `timelake maintain` and `REFRESH MATERIALIZED ROLLUP` refresh every time. Inline maintenance after an append
  refreshes a rollup once its last refresh is older than `timelake.rollup-refresh-interval` (default 1 minute, `off`
  for never), so a busy table doesn't commit to its rollups on every append.
- Two refreshes at once are safe: the later commit wins each bucket, and its record names the raw snapshot those rows
  reflect, so any bucket changed after that snapshot counts as changed again.
- If the recorded raw snapshot is no longer in the history of the raw table's current snapshot (a rollback past it),
  the rollup may hold rows the raw table no longer has. Routing skips it, and the next refresh removes its files,
  records that it reflects nothing, and computes it again.

**Routing.** The Timelake reader answers `SELECT … GROUP BY time_bucket(w, time), keys…` on a raw table from a
rollup when the grouping is the rollup's (same width, same key columns), every aggregate is one the rollup stores,
and the filters are on the time column (comparisons with constants, `now()` included) or on the grouping columns.
Grouping columns may be filtered by another table (`tag IN (SELECT tag FROM tags WHERE line = 'L0002')`, which
DataFusion plans as a semi-join): the rollup's rows go through the same semi-join (built 2026-10-20). IIoT small,
daily statistics for the tags of one line from an hourly rollup: 551 → 37 ms.
The answer is exact, not a stale rollup:
- Buckets that are inside the query's time range, unchanged since the refresh, and within both tables' retention come
  from the rollup.
- Every other row (the changed buckets, the partial buckets at the edges of the time range) is aggregated from the raw
  table as usual. The two parts share no bucket, so they are combined with `UNION ALL`.
- `timelake.rollup-routing` (table and session; default on) turns it off. A session that reads without dedup
  (`timelake.read_dedup = false`) is not routed, since rollups count each reading once.

**Not in the first version:** rollups of rollups, aggregates that need stored state (percentiles, time-weighted
averages), answering a coarser query (hourly from minutes) or fewer keys from a rollup, calendar buckets (months),
filters in the definition, routing in plain DuckDB, and recomputing buckets after a delete ([spec 17](0017-deletes.md)).

#### Coarser queries (built 2026-10-18)
Routing also answers a query whose bucket is a whole multiple of the rollup's and whose keys are a subset of the
rollup's. The rollup's rows in the query's buckets are aggregated again: `count` and `sum` by `sum`, `min` and `max` by
themselves, `first_value` and `last_value` ordered by the bucket, and `avg` as `sum(sum) / sum(count)`, or
`sum(avg × count) / sum(count)` when the rollup stores the average and a count of the column (`count(*)` where the
column can't be null). A query bucket comes from the rollup only if every rollup bucket in it is unchanged since the
refresh; the rest come from the raw table as before. A query at the rollup's own width and keys reads its rows as
they are. Measured on IIoT small (30 days, 1,000 tags, 1-minute rollup of 30 M rows): daily statistics per tag 2.07 →
0.25 s, 6-hourly totals over all tags 1.10 → 0.11 s, hourly for 20 tags 144 → 66 ms; answers equal the raw table's
to floating-point rounding (relative difference at most 9 × 10⁻¹³).

**Rollups of rollups** come from the same routing: every rollup is defined on the raw table, and its refresh query
(a `GROUP BY time_bucket` over the changed buckets) is routed like any query to the table's other rollups, leaving out
the one being refreshed. Refreshes go from the narrowest bucket to the widest, so a daily rollup is computed from an
up-to-date hourly or minute one. IIoT small: creating a daily rollup 5.2 s from the raw table, 1.8 s with a 1-minute
rollup.

#### Time ranges without a bucket, and which rollup (built 2026-10-19)

A query grouped by some of a rollup's key columns and by no `time_bucket`, over a time range, is rewritten as the same
query per bucket of a rollup's width, with its aggregates in mergeable form (`count`, `sum`, `min`, `max`; `avg` as
`sum` and `count`; `m4`; the signal and gap aggregates as their states, `time_weighted_state` and `gap_state`), routed
as a bucketed query, and merged once more by the query's keys. At a rollup's own width and keys, stored states are
read as they are, not merged per bucket (30 M one-row groups took 16 s). A rollup may also store
`time_weighted_state` and `gap_state` directly. IIoT small, an hourly rollup, 30 days per tag: statistics 1.75 s →
55 ms, gaps 1.34 s → 44 ms, time-weighted means 1.50 s → 0.15 s; a day of statistics 93 → 39 ms. Same answers up to
the rounding of sums.

**Which rollup:** the widest that answers (fewest rows), then the next. A rollup answers from stored signal states
only where it holds at least 32 raw rows per row on average, and from `m4` points or gap runs at 4: merging an 80-byte
state costs about 25–50 readings. Measured on IIoT small: daily time-weighted means from a 1-minute rollup (12 readings
a row) 3.6 s against 2.0 s from the readings (the coarser routing of 0.7.0 did this), daily averages from it 0.29 s
against 1.9 s, gaps over 30 days 0.24 s against 1.2 s. The counts come from both tables' live files.

**Gaps:** `gap_count` and `gap_seconds` are stored as their state for one threshold, named with it
(`gap_count:300s`), which answers both at that threshold. Default output names now replace `:` with `_`
(`integral_linear_value`, `gap_count_300s`).

## Open questions and issues

- [#15](https://github.com/polythings/timelake/issues/15) Rollups: refresh rules (overwrite buckets, ignore compaction and retention, check lag vs retention)
- [#108](https://github.com/polythings/timelake/issues/108) Question: where rollup definitions live, table properties or the catalog
- [#136](https://github.com/polythings/timelake/issues/136) Rollups: what the first versions leave out

## History

- 2026-10-16: first version built (P7)
- 2026-10-18: coarser queries, fewer keys, rollups of rollups
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: was spec 21; specs regrouped one per feature, with milestones on the roadmap.
- 2026-10-19: time ranges without a bucket, the widest rollup first, readings per row for states, gap states.
- 2026-10-20: grouping columns filtered by another table (`IN (SELECT …)`) route too.
