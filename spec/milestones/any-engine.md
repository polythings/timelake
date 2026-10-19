---
number: 10
title: Any engine can attach
status: planned
updated: 2026-10-19
description: An Iceberg REST catalog, so Snowflake, Trino, Athena, and Spark query tables in place, and a DuckDB extension with Timelake's speedups and functions.
specs: [32, 33, 34, 35]
---

# Any engine can attach

**Planned** · updated 2026-10-19

## Summary

Plain engines already read Timelake tables from the bucket. This milestone lets them attach the way they expect: an
Iceberg REST catalog that Snowflake, Trino, Athena and Spark connect to, a DuckDB extension with dedup on read and the
function library, Snowflake set up in one command, and documented patterns for many tenants.

<!-- specs:start -->

## Specs

| # | Spec | Status | Versions |
|---:|---|---|---|
| 32 | [REST catalog](../0032-rest-catalog.md) | Planned | – |
| 33 | [DuckDB extension](../0033-duckdb-extension.md) | Planned | – |
| 34 | [Snowflake connection](../0034-snowflake.md) | Planned | – |
| 35 | [Multitenancy](../0035-multitenancy.md) | Planned | – |

<!-- specs:end -->

## History

- 2026-10-19: milestone written when the specs were regrouped one per feature.
