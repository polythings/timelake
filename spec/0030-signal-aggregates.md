---
number: 30
title: Signal aggregates
area: SQL functions
status: done
updated: 2026-10-19
versions: [0.7.0, unreleased]
related: [26, 27, 28, 31]
---

# 30. Signal aggregates

**Done** · updated 2026-10-19 · versions 0.7.0, unreleased · milestone: [Time-series SQL](milestones/time-series-sql.md)
· related: [spec 26](0026-series-order.md), [spec 27](0027-rollups.md), [spec 28](0028-function-library.md), [spec 31](0031-m4.md)

## Summary

`time_weighted_mean`, `integral`, `counter_diff` and `counter_rate` over irregular readings, exact, as fast as `avg`. They share one mergeable state (runs of readings), which rollups store and several aggregates of one column compute once; a condition counts as 1 while true, for time in a state and uptime.

## Design

`time_weighted_mean(time, value [, method])` and `integral(time, value [, method])` (value × seconds), method
`forward_fill` (default, alias `ffill`) or `linear`; `counter_diff(time, value)` and `counter_rate(time, value)` (per
second), where a drop is a reset to zero, so the reading after it counts whole. All over each group's first to last
reading; readings at one time count once with the largest value; one reading time gives its value as the mean, 0 as
the integral and growth, and no rate. Decided with the user on 2026-10-18 (exact always, both methods, first to last
reading).
- **State:** a list of segments, runs of a group's readings in time order: first and last reading, the values of the
  readings next to them, the time from the first to the next and into the last, the area and the counter's growth.
  Segments that follow each other join exactly (the gap's area and growth added; a shared time takes the larger value,
  corrected through the stored neighbours); partial results emit their runs unjoined, since another partition may
  hold readings between them. Overlapping segments are an error. The four aggregates share the state and differ in
  what they return from it. Layout measured on IIoT hourly means: neighbours as `Option<f64>` (96-byte segments)
  2.30 s, as plain `f64`s (80 bytes) 2.17 s, the same as the mean alone before; the open segment kept inline in each
  group instead of behind the vector's pointer 2.95 s (the group array doubles and DataFusion moves it as it grows).
- **Series order:** no partition may have readings of a series inside another partition's run of it. Scans under a
  signal aggregate read each series whole and in time order, merging only the files that need it
  ([spec 26](0026-series-order.md), setting `series-order`).
- **Rollups** store the state (`time_weighted_state`); routing merges it with the query's aggregate
  (`integral(state [, method])`), at the rollup's width too. One stored state answers all four for its column and
  method; the counters from either method's state.
- **Cost:** IIoT small, hourly per tag over 30 days: `avg` 1.98 s; each signal aggregate 2.18–2.20 s; all four in one
  query 3.47 s with a state each, 2.11 s sharing one: an optimizer rule after routing replaces the signal aggregates
  of the same readings and method (the counters join either method's) with one `time_weighted_state` and computes
  each result from it in a projection (`integral_of_state(state)`); setting `shared-signal-state`. Against the window form of the IIoT queries over 30 days: energy
  per machine (`lag`) 1.66 → 1.09 s, time-weighted means of a line's temperatures (`lead`) 166 → 87 ms.

## Open questions and issues

None open.

## History

- 2026-10-18: `time_weighted_mean` built
- 2026-10-19: `integral`, `counter_diff`, `counter_rate`, conditions, shared state, scans in stretches of time
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: was spec 22; specs regrouped one per feature, with milestones on the roadmap.
