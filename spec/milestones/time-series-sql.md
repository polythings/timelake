---
number: 6
title: Time-series SQL
status: in-progress
versions: 0.5.0–
updated: 2026-10-19
description: Rollups that answer queries exactly, time-weighted averages, counters, downsampling, gap filling, and the rest of the function library.
specs: [27, 28, 29, 30, 31, 26]
---

# Time-series SQL

**In progress** · versions since 0.5.0 · updated 2026-10-19

## Summary

The queries time-series users write every day, exact and fast: rollups kept up to date and used automatically,
time-weighted means and integrals of irregular readings, counters across resets, time in a state, downsampling with
`m4`, gap filling, and gaps in the readings. Built so far: everything but the planned functions of the library
(quantiles, ASOF and the rest, [spec 28](../0028-function-library.md)) and percentiles in rollups.

<!-- specs:start -->

## Specs

| # | Spec | Status | Versions |
|---:|---|---|---|
| 27 | [Materialized rollups](../0027-rollups.md) | Done | 0.5.0, 0.7.0, unreleased |
| 28 | [Function library](../0028-function-library.md) | In progress | 0.1.0, 0.7.0, unreleased |
| 29 | [Time buckets and gap filling](../0029-gap-filling.md) | Done | 0.1.0, unreleased |
| 30 | [Signal aggregates](../0030-signal-aggregates.md) | Done | 0.7.0, unreleased |
| 31 | [Downsampling with m4](../0031-m4.md) | Done | 0.7.0 |
| 26 | [Scans in series order](../0026-series-order.md) | Done | 0.7.0, unreleased |

<!-- specs:end -->

## History

- 2026-10-16: rollups, first version (0.5.0; P7 of the performance plan)
- 2026-10-18: coarser queries and rollups of rollups, `time_weighted_mean`, `m4` (0.7.0)
- 2026-10-19: `integral`, counters, conditions, shared state, gap filling; milestone written when the specs were
  regrouped one per feature.
