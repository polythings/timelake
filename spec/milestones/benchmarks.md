---
number: 8
title: Benchmarks against every alternative
status: in-progress
updated: 2026-10-19
description: Every run kept with its commit, a weekly run on a fixed machine, and an industrial IoT benchmark against InfluxDB, TimescaleDB, ClickHouse and QuestDB with the monthly cost of each.
specs: [44, 45]
issues: [143]
---

# Benchmarks against every alternative

**In progress** · updated 2026-10-19

## Summary

Benchmarks anyone can rerun, against every alternative, that publish where Timelake loses as well as where it wins and
report cost and growth next to latency (D21): the timebench harness with TSBS IoT and DevOps, every run on the website
with its commit, a weekly run on one fixed machine, and an industrial IoT workload with checked answers. Still open:
the public benchmarks not yet run, expert-written Iceberg, a run per object store, and side-by-side demos on the
website.

<!-- specs:start -->

## Specs

| # | Spec | Status | Versions |
|---:|---|---|---|
| 44 | [Benchmark harness: timebench](../0044-timebench.md) | In progress | – |
| 45 | [Industrial IoT benchmark](../0045-iiot-benchmark.md) | In progress | – |

<!-- specs:end -->

## History

- 2026-10-15: timebench, the IIoT workload and the weekly run built
- 2026-10-19: milestone written when the specs were regrouped one per feature.
