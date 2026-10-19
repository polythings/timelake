---
number: 29
title: Time buckets and gap filling
area: SQL functions
status: done
updated: 2026-10-19
versions: [0.1.0, unreleased]
related: [27, 28]
issues: [59]
---

# 29. Time buckets and gap filling

**Done** · updated 2026-10-19 · versions 0.1.0, unreleased · milestones: [v0: a maintenance-free time-series table on S3](milestones/v0.md), [Time-series SQL](milestones/time-series-sql.md)
· related: [spec 27](0027-rollups.md), [spec 28](0028-function-library.md)

## Summary

`time_bucket` groups readings into fixed-width buckets; the table function `time_buckets(width, start, end [, origin])`
lists every bucket of a range, empty ones included, to join readings against; and the window functions `forward_fill`,
`backward_fill` and `interpolate` fill the empty ones. Together they give one row per bucket for charts and reports.

## Design

- **`time_bucket(width, time [, origin])`** (also `date_bin`): fixed widths; built in v0. Rollups group by it
  ([spec 27](0027-rollups.md)). Calendar buckets (months), offsets and time zones are not built.
- **`time_buckets(width, start, end [, origin])`** (pandas `date_range`): a table function returning one column,
  `bucket`, from the bucket holding `start` (aligned like `time_bucket`, to the Unix epoch or `origin`) to the last one
  starting before `end`. Arguments may be expressions (`now() - INTERVAL '1 day'`), folded before planning. At most
  10 million buckets, a guard against a width typed in the wrong unit; months are rejected. Joined against a grouped
  query, it keeps empty buckets, which a window function then fills:
  ```sql
  SELECT b.bucket, forward_fill(h.temp) OVER (ORDER BY b.bucket) AS temp
  FROM time_buckets(INTERVAL '1 hour', now() - INTERVAL '1 day', now()) b
  LEFT JOIN (SELECT time_bucket(INTERVAL '1 hour', time) AS bucket, avg(temp) AS temp
             FROM iot.readings WHERE device = 'pump-7' GROUP BY 1) h ON h.bucket = b.bucket
  ```
- **`forward_fill(x)`** (`ffill`), **`backward_fill(x)`** (`bfill`): window functions carrying the last (next)
  non-null value over nulls, in the window's order. **`interpolate(x [, t])`**: linear between the neighbouring
  non-null values, by row position or, given `t`, by time (pandas' `linear` and `time`). Rows before the first value
  (and, for `backward_fill` and `interpolate`, after the last) stay null. Each computes a whole partition at once (an
  index of the source row per output row, then one take): `forward_fill` over 10 M rows 0.26 s against DataFusion's
  `last_value(x) IGNORE NULLS` 3.6 s, `backward_fill` 0.27 s against `first_value(x) IGNORE NULLS` 0.37 s
  ([`bench/results/PERFORMANCE.md`](../bench/results/PERFORMANCE.md), P7 gap filling).

## Open questions and issues

- [#59](https://github.com/polythings/timelake/issues/59) Query: cap the number of time buckets

## History

- 2026-10-12: `time_bucket` in v0
- 2026-10-19: `time_buckets`, `forward_fill`, `backward_fill`, `interpolate`; moved from SPEC.md into spec 24, then
  into this spec when the specs were regrouped one per feature.
