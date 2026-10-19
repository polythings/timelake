---
number: 34
title: Agent benchmark
area: Benchmarks
status: planned
updated: 2026-10-15
related: [36, 33]
---

# 34. Agent benchmark

**Planned** · updated 2026-10-15
· related: [spec 36](0036-agent-access.md), [spec 33](0033-iiot-benchmark.md)

## Summary

Does an LLM agent answer questions about time-series data more correctly, with fewer tokens and tool calls, when the data is in Timelake? One agent loop against the same data through different arms, scored on generated tasks with known answers.

## Design

Most people now ask questions of their data through LLM agents. The question: does an agent answer more correctly, with fewer tokens and tool calls, when the data is in Timelake? TSRBench doesn't answer it: its models get the series as text or a plot, never through tools. Benchmarks where agents do use tools are close but not it: OpenRCA (root cause over raw telemetry files), AssetOpsBench (industrial assets behind MCP servers), AgentFuel (IoT and telecom questions), Grafana's o11y-bench (answers checked against live queries), DataAgentBench (several databases).

- **Design:** one agent loop (same model, prompt, turn limit and seed) against the same data in different arms: raw files with a Python sandbox; DuckDB over Parquet; TimescaleDB (same column comments for metadata); Timelake through generic SQL tools; Timelake through its MCP tools (F1, [spec 36](0036-agent-access.md)). Comparing arms separates the engine from the tool design; switching single features off (metadata, downsampling, helpful errors, result caps) shows what each is worth.
- **Tasks:** generated from the workload generators with injected events whose answers are known: lookups with units, windowed aggregates, comparing periods, anomalies with their time ranges, a root cause shared by many devices, gaps vs zeros, late data and "as of" questions, counters across resets, long trends that only fit downsampled, and questions that must be refused.
- **Scoring:** a typed `submit_answer` (value, unit, entities, interval), checked with tolerances; an LLM judge only for the explanation. Per task: accuracy, tokens, tool calls and errors, bytes returned to the model, time and dollars.
- **Then:** run OpenRCA's metric tasks and AssetOpsBench's IoT scenarios against Timelake as published comparisons.

## Open questions and issues

None open.

## History

- 2026-10-15: planned, design to agree (SPEC.md §17.8)
- 2026-10-19: moved from SPEC.md into its own spec.
