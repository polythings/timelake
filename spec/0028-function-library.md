---
number: 28
title: Function library
area: SQL functions
status: in-progress
updated: 2026-10-20
versions: [0.1.0, 0.7.0, unreleased]
related: [29, 30, 31, 33]
issues: [124, 125, 126, 127]
---

# 28. Function library

**In progress** · updated 2026-10-20 · versions 0.1.0, 0.7.0, unreleased · milestone: [Time-series SQL](milestones/time-series-sql.md)
· related: [spec 29](0029-gap-filling.md), [spec 30](0030-signal-aggregates.md), [spec 31](0031-m4.md), [spec 33](0033-duckdb-extension.md)

## Summary

Time-series SQL functions with descriptive names and pandas aliases: bucketing, gap filling, selection, statistics,
quantiles, changes, time-weighted aggregates, downsampling, rolling windows. Functions with a design of their own have
their own spec; the rest are listed here with their issues. Built so far: `time_bucket`, `time_buckets`, the fills,
the signal aggregates, the gap aggregates, `m4`, and `first`, `last` and `value_at`.

## Design

Every function has a plain, descriptive name. Where pandas uses a different name, that name works too, as an alias:
`forward_fill` and `ffill` are the same function (D22). Polars mostly uses the descriptive names, so they match it.

| Category | Functions (pandas alias in brackets, where it differs) | Status |
|----------|-----------|---|
| Bucketing | `time_bucket`; `time_buckets(width, start, end [, origin])`, every bucket in a range (`date_range`) ([spec 29](0029-gap-filling.md)) | Built (fixed widths) |
| Gap filling | `forward_fill` (`ffill`), `backward_fill` (`bfill`), `interpolate` ([spec 29](0029-gap-filling.md)) | Built |
| Time-weighted | `time_weighted_mean` (`forward_fill` or `linear`), `integral` ([spec 30](0030-signal-aggregates.md)). **Critical for IoT** because sampling is irregular | Built |
| Changes | `counter_diff` and `counter_rate`, across counter resets ([spec 30](0030-signal-aggregates.md)) | Built |
| States and uptime | Time in a state and uptime: `integral(time, condition)` and `time_weighted_mean(time, condition)`, a condition counting 1 while true ([spec 30](0030-signal-aggregates.md); instead of `state_duration` and `uptime`) | Built |
| Downsampling | `m4(time, value)`, a group's first, smallest, largest and last points ([spec 31](0031-m4.md)) | Built |
| Gaps | `gap_count(time, threshold)` and `gap_seconds(time, threshold)`: how often and how long the readings stopped for longer than a threshold ([spec 30](0030-signal-aggregates.md)) | Built |
| Selection | `first(time, value)`, `last(time, value)`, `value_at(time, value, at)`, the latest value at or before a time (`asof`); ASOF JOIN | Built, names for ordered `first_value` and `last_value`; ASOF JOIN planned (#124) |
| Quantiles | `quantile` (exact, pandas' linear interpolation), `approx_quantile` (a t-digest: mergeable sketch state, so it could roll up across buckets) | Built; a rollup storing the digest is planned (#125) |
| Statistics | `mean`, `stddev` (`std`), `variance` (`var`), `skewness` (`skew`), `kurtosis` (`kurt`), `corr`, `covariance` (`cov`), `regr_slope`, `regr_intercept`, `regr_r2`, `histogram` | Built but `histogram` (#126): DataFusion's, with the pandas names added; `skewness` and `kurtosis` from mergeable moments, as pandas defines them |
| Changes | `diff`, `percent_change` (`pct_change`), `rate(time, x)` (per second), window functions | Built |
| Finance | `ohlc(time, price)` (a struct of open, high, low, close; volume is a `sum`) | Built |
| Downsampling | `lttb(time, value, n)` | Built (keeps a group's readings; `m4` is the mergeable one) |
| Rolling and anomalies | `rolling_mean(x, n)`, `rolling_std(x, n)` (`rolling(n).mean()`), `zscore`, flatlines: `flatline_count(time, value, threshold)`, `flatline_seconds(…)` (stuck sensors) | Built: running sums, 4× faster than a framed `avg` over 60 rows; flatlines as run aggregates, 34× faster than a window query |

**Availability by engine:**
- **Timelake reader (DataFusion):** Full library as Rust UDFs/UDAFs.
- **Plain DuckDB:** Functions DuckDB has natively (`time_bucket`, `arg_max`, ASOF JOIN, `approx_quantile`) plus a
  published **SQL macro pack** (#127). The macros must use fixed-width intervals (e.g. `'24 hours'`, not `'1 day'`) or
  `date_trunc` for UTC data. Calendar-unit buckets on `timestamptz` take a ~130× slower path in DuckDB (measured in
  [spec 44](0044-timebench.md)).
- **DuckDB extension (later):** Full parity ([spec 33](0033-duckdb-extension.md)).

## Decisions

| ID | Decision | Status |
|---|---|---|
| D22 | Every time-series function has a descriptive name, and its pandas name as an alias where that differs (`forward_fill` / `ffill`, `value_at` / `asof`); "continuous aggregates" are called **materialized rollups** ([spec 27](0027-rollups.md)) | Accepted |

## Open questions and issues

- [#124](https://github.com/polythings/timelake/issues/124) Functions: `value_at` (`asof`) and ASOF JOIN
- [#125](https://github.com/polythings/timelake/issues/125) Functions: `quantile` and `approx_quantile` with a mergeable sketch
- [#126](https://github.com/polythings/timelake/issues/126) Functions: the rest of the library (`ohlc`, `lttb`, rolling, `zscore`, flatline, changes, statistics aliases)
- [#127](https://github.com/polythings/timelake/issues/127) DuckDB macro pack for the function library

## History

- 2026-10-11: function library and names (SPEC.md §11, D22)
- 2026-10-12: `time_bucket` in v0
- 2026-10-18: `time_weighted_mean`, `m4`
- 2026-10-19: `integral`, the counters, conditions, `time_buckets` and the fills; moved from SPEC.md into spec 24,
  then into this spec when the specs were regrouped one per feature.
- 2026-10-19: `gap_count` and `gap_seconds` (spec 30).
- 2026-10-20: `first`, `last` and `value_at` (`asof`) built (#124).
- 2026-10-20: the pandas statistics names, `skewness` and `kurtosis`, `diff`, `percent_change`, `rate`, `rolling_mean`,
  `rolling_std` and `zscore` (#126).
- 2026-10-20: `flatline_count` and `flatline_seconds` (#126).
- 2026-10-20: `quantile` and `approx_quantile` (#125).
- 2026-10-20: `ohlc` and `lttb` (#126).
