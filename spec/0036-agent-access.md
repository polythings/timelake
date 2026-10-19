---
number: 36
title: Agent access with permission sets
area: Services and planned features
status: planned
updated: 2026-10-13
related: [29, 34, 17]
issues: [111]
---

# 36. Agent access with permission sets

**Planned** · updated 2026-10-13
· related: [spec 29](0029-multitenancy.md), [spec 34](0034-agent-benchmark.md), [spec 17](0017-read-paths.md)

## Summary

An MCP server and client operations that let LLM agents find, understand and read time-series data, within permission sets the reader enforces by rewriting plans, with results that fit a context window and every query audited.

## Design

LLM agents should be able to find, understand and read time-series data without a human writing SQL for them, and only see what they are allowed to.

- **Interface:** an MCP server (`timelake mcp`, local over stdio or hosted over HTTP) plus the same operations in the Python and TypeScript clients. Tools: list tables, describe a table (schema, contract, units, series-key meaning, time range), latest values, range query with a bucket and aggregate, and SQL.
- **Self-describing data:** column descriptions and units are stored as table properties, so an agent gets context instead of guessing what `v3` means.
- **Fits in a context window:** results are capped by rows and bytes; long ranges are downsampled automatically (`time_bucket`, `lttb`), and the response says it was downsampled. Every query has a scan budget (bytes, time) so a bad question can't run up an S3 bill.
- **Permission sets:** a named policy stored in the lake (`_timelake/policies/<name>.json`) lists allowed tables, columns, row filters (for example `site = 'plant-3'`, or a tenant), the maximum time window, and operations. Agents only ever get read access. The reader enforces a policy by rewriting the plan (row filter and column projection injected into the DataFusion plan) before it runs. An agent's credential names exactly one permission set.
- **The honest limit:** a policy enforced by the reader only binds clients that go through Timelake. Anyone holding raw bucket credentials can read the files. So the agent never receives bucket credentials: the MCP server holds them, or the REST catalog vends prefix-scoped credentials ([spec 29](0029-multitenancy.md)).
- **Audit:** every agent query is logged with its permission set to an Iceberg table, consistent with D6.

## Open questions and issues

- [#111](https://github.com/polythings/timelake/issues/111) Question: agent permission sets, open source or premium, and the policy format

## History

- 2026-10-13: planned (F1, SPEC.md §21.1)
- 2026-10-19: moved from SPEC.md into its own spec.
