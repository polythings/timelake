---
number: 26
title: Scans in series order
area: Reading
status: done
updated: 2026-10-19
versions: [0.7.0, unreleased]
related: [12, 21, 30]
---

# 26. Scans in series order

**Done** · updated 2026-10-19 · versions 0.7.0, unreleased · milestone: [Time-series SQL](milestones/time-series-sql.md)
· related: [spec 12](0012-late-data-and-dedup.md), [spec 21](0021-reader.md), [spec 30](0030-signal-aggregates.md)

## Summary

Queries that work through each series in time order (signal aggregates, and windows partitioned by the series key and
ordered by time) read files so that each series arrives whole and in order: files in time order, each scan partition a
contiguous stretch of time, and only the files that overlap merged in series order. No repartitioning or sorting.

## Design

- **Signal and gap aggregates** ([spec 30](0030-signal-aggregates.md)) need it for correctness: no partition may have readings
  of a series inside another partition's run of it. Files that must be merged (copies with dedup on read; without,
  files that overlap on `(series…, time)` or aren't sorted) are merged in series order with every file of the time they
  span (closed under overlap; the dedup operator, keeping every row without dedup), and the files before and after
  that time are read as they are, files in time order, each partition a contiguous stretch. DataFusion 54's work
  stealing between file partitions is turned off for these scans with `preserve_order`.
  - **Setting `series-order`** (named `time-weighted-mean` in 0.7.0): `auto`, `segments` (copies only; overlaps are an
    error), `merge` (every file).
  - 0.7.0 spread files over partitions by size and merged only tables with dedup off, so a partition's run spanned days
    other partitions held, and `GROUP BY tag` over a month failed with "runs that overlap in time".
  - Measured alternative: rewriting to windows (`lead` per group in time order): hourly means of 370 M IIoT readings
    550 s and 20 GB peak, against 2.0 s with segments.
- **Windows** partitioned by the series key and ordered by time (`lead(…) OVER (PARTITION BY <series key> ORDER BY
  time)`): the scan declares the series hash partitioning and the time order, so DataFusion neither repartitions nor
  sorts. **Setting `window-series-order`** (`auto`, `true`, `false`). `auto` merges when the files are in one time
  partition: IIoT `gaps` 260 → 215 ms, `time-above-limit` 396 → 327 ms. Over a week of files merging is slower (2.0 →
  3.0 s), so `auto` leaves those.

## Open questions and issues

None open.

## History

- 2026-10-18: series order for `time_weighted_mean` (0.7.0)
- 2026-10-19: time-ordered stretches per partition, merging only the span that needs it; windows in series order;
  moved from SPEC.md (specs 20 and 22) into this spec when the specs were regrouped one per feature.
