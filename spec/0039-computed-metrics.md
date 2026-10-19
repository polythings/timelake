---
number: 39
title: Computed metrics
area: Services and planned features
status: planned
updated: 2026-10-13
related: [21, 37, 38]
issues: [114]
---

# 39. Computed metrics

**Planned** · updated 2026-10-13
· related: [spec 21](0021-rollups.md), [spec 37](0037-read-models.md), [spec 38](0038-data-to-events.md)

## Summary

User-defined Python functions whose results Timelake keeps up to date the way it keeps rollups: recomputing the windows each commit affects.

## Design

Let users define a Python function and the result it produces, for example a computed metric, and have Timelake keep that result up to date automatically, the way materialized rollups stay up to date.

- **Definition:** the function, its inputs (tables, series selection, window) and its output table with a normal contract. Sketch:
  ```python
  @tl.metric(inputs={"flow": "iot.flow", "power": "iot.power"},
             output="iot.pump_efficiency", window="1m", series="pump_id")
  def efficiency(flow: pl.DataFrame, power: pl.DataFrame) -> pl.DataFrame: ...
  ```
- **Incremental like rollups:** on each commit, compute which output windows are affected (snapshot diff), pass the inputs as Arrow (zero-copy into Polars or pandas), and replace those windows in the output table. Late data recomputes the affected windows the same way it does for rollups.
- **Contract for functions:** deterministic and per-window by default, so any window can be recomputed. State carried across windows is opt-in.
- **Where it runs:** in the writer's process (Python bindings), in a scheduled `timelake maintain` run, or in a Lambda. There is still no server. A hosted version needs sandboxing.
- **Composes:** computed metrics are ordinary tables, so they can feed read models (F2) and events (F3). SQL and Rust functions can follow on the same mechanism.

## Open questions and issues

- [#114](https://github.com/polythings/timelake/issues/114) Question: running user Python safely in a hosted service

## History

- 2026-10-13: planned for later (F4, SPEC.md §21.4)
- 2026-10-19: moved from SPEC.md into its own spec.
