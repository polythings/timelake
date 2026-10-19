---
number: 24
title: Function library
area: Reading
status: in-progress
updated: 2026-10-19
related: [22, 23, 21]
issues: [59, 123, 124, 125, 126, 127]
---

# 24. Function library

**In progress** · updated 2026-10-19
· related: [spec 22](0022-signal-aggregates.md), [spec 23](0023-m4.md), [spec 21](0021-rollups.md)

## Summary

Time-series functions with descriptive names and pandas aliases: bucketing, gap filling, selection, statistics, quantiles, changes, time-weighted aggregates, downsampling, rolling windows. Built so far: `time_bucket`, `time_buckets`, the fills, the signal aggregates and `m4`.

## Design

Every function has a plain, descriptive name. Where pandas uses a different name, that name works too, as an alias:
`forward_fill` and `ffill` are the same function (D22). Polars mostly uses the descriptive names, so they match it.

| Category | Functions (pandas alias in brackets, where it differs) |
|----------|-----------|
| Bucketing | `time_bucket` (with origin, offset, timezone, months); `time_buckets(width, start, end [, origin])`, every bucket in a range, including empty ones, to join against (`date_range`; built 2026-10-19, fixed widths) |
| Gap filling | `forward_fill` (`ffill`), `backward_fill` (`bfill`), `interpolate` (linear or by time); built 2026-10-19 as window functions over a partition at once: `forward_fill` over 10 M rows 0.26 s against DataFusion's `last_value(x) IGNORE NULLS` 3.6 s, `backward_fill` 0.27 s against `first_value(x) IGNORE NULLS` 0.37 s |
| Selection | `first`, `last`, `value_at`, the latest value at or before a time (`asof`); ASOF JOIN |
| Statistics | `mean`, `stddev` (`std`), `variance` (`var`), `skewness` (`skew`), `kurtosis` (`kurt`), `corr`, `covariance` (`cov`), `regr_slope`, `regr_intercept`, `regr_r2`, `histogram` |
| Quantiles | `quantile`, `approx_quantile` (mergeable sketch state, so it rolls up across buckets) |
| Changes | `diff`, `percent_change` (`pct_change`), `rate` (per second); `counter_diff` and `counter_rate` also handle counter resets ([spec 22](0022-signal-aggregates.md), built) |
| Time-weighted | `time_weighted_mean` (`forward_fill` or `linear`), `integral` ([spec 22](0022-signal-aggregates.md), built). **Critical for IoT** because sampling is irregular |
| States and uptime | Time in a state and uptime: `integral(time, condition)` and `time_weighted_mean(time, condition)`, a condition counting 1 while true (built 2026-10-19 instead of `state_duration` and `uptime`; IIoT `time-above-limit` 334 → 191 ms against the `lead()` window form); `gaps` (silences longer than a threshold) |
| Finance | `ohlc` (open, high, low, close; volume is a `sum`) |
| Downsampling | `m4(time, value)`, a group's first, smallest, largest and last points ([spec 23](0023-m4.md), built); `lttb` |
| Rolling and anomalies | `rolling_mean`, `rolling_std` (`rolling(…).mean()`), `zscore`, flatline (stuck sensor) detection |

**Availability by engine:**
- **Timelake reader (DataFusion):** Full library as Rust UDFs/UDAFs.
- **Plain DuckDB:** Functions DuckDB has natively (`time_bucket`, `arg_max`, ASOF JOIN, `approx_quantile`) plus a published **SQL macro pack**. The macros must use fixed-width intervals (e.g. `'24 hours'`, not `'1 day'`) or `date_trunc` for UTC data. Calendar-unit buckets on `timestamptz` take a ~130× slower path in DuckDB (measured in [spec 32](0032-timebench.md)).
- **DuckDB extension (later):** Full parity.

## Decisions

| ID | Decision | Status |
|---|---|---|
| D22 | Every time-series function has a descriptive name, and its pandas name as an alias where that differs (`forward_fill` / `ffill`, `value_at` / `asof`); "continuous aggregates" are called **materialized rollups** ([spec 21](0021-rollups.md)) | Accepted |

## Open questions and issues

- [#59](https://github.com/polythings/timelake/issues/59) Query: cap the number of time buckets
- [#123](https://github.com/polythings/timelake/issues/123) Functions: `gaps(time, threshold)`, silences longer than a threshold
- [#124](https://github.com/polythings/timelake/issues/124) Functions: `value_at` (`asof`) and ASOF JOIN
- [#125](https://github.com/polythings/timelake/issues/125) Functions: `quantile` and `approx_quantile` with a mergeable sketch
- [#126](https://github.com/polythings/timelake/issues/126) Functions: the rest of the library (`ohlc`, `lttb`, rolling, `zscore`, flatline, changes, statistics aliases)
- [#127](https://github.com/polythings/timelake/issues/127) DuckDB macro pack for the function library

## History

- 2026-10-11: library specified (SPEC.md §11.2)
- 2026-10-18: `m4`, `time_weighted_mean`
- 2026-10-19: gap filling, signal aggregates, time in a state
- 2026-10-19: moved from SPEC.md into its own spec.
