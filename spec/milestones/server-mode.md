---
number: 16
title: Server mode
status: exploring
updated: 2026-10-19
description: Timelake as a database on one machine, with the bucket as the copy. Then TSBS run with its own tools, next to published results.
specs: [43]
---

# Server mode

**Exploring** · updated 2026-10-19

## Summary

An experiment first: the same engine as one long-lived server on local NVMe, with the bucket as the durable copy and
Postgres wire and Flight SQL protocols, measured against TimescaleDB before anything is built. It also lets TSBS run
with its own loader and query runner, for numbers next to published results.

<!-- specs:start -->

## Specs

| # | Spec | Status | Versions |
|---:|---|---|---|
| 43 | [Server mode](../0043-server-mode.md) | Planned | – |

<!-- specs:end -->

## History

- 2026-10-19: milestone written when the specs were regrouped one per feature.
