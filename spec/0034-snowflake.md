---
number: 34
title: Snowflake connection
area: Integrations
status: planned
updated: 2026-10-13
versions: []
related: [3, 27, 28]
issues: [115]
---

# 34. Snowflake connection

**Planned** · updated 2026-10-13 · milestone: [Any engine can attach](milestones/any-engine.md)
· related: [spec 3](0003-object-store-catalog.md), [spec 27](0027-rollups.md), [spec 28](0028-function-library.md)

## Summary

Snowflake queries Timelake tables in place as Iceberg tables, through the REST catalog or registered metadata files, set up by one command, with time-series helpers mapped onto Snowflake built-ins.

## Design

Pitch: **an optimized time-series store for your Snowflake.** Many data teams, including large oil and gas operators, already land historian and sensor data in Snowflake. Timelake lets them keep that data in their own bucket, laid out for time series, and query it from Snowflake with no copy and no Snowpipe.

- **What the user gets:** Snowflake sees Timelake tables as Iceberg tables. Data never moves into Snowflake storage, so there is no ingest pipeline into Snowflake, no Snowflake storage bill for raw telemetry, and the same tables still serve DuckDB, Spark, dashboards and agents.
- **Why it is faster and cheaper than plain Iceberg in Snowflake:** Snowflake prunes Iceberg files on manifest column stats. Timelake files are sorted by (series, time), compacted, and time-partitioned, so a query for one asset or a recent window touches few files and burns fewer credits. Rollups are Iceberg tables too, so dashboards in Snowflake can read the 1-minute or 1-hour table instead of raw data.
- **Two ways to connect:**
  - **Through the REST catalog (preferred):** Snowflake attaches the Timelake catalog through a catalog integration, discovers tables, and picks up new snapshots automatically. Depends on `timelake-catalog` ([spec 32](0032-rest-catalog.md)).
  - **Without a catalog server:** each table is registered in Snowflake from its metadata file in object storage, and Timelake refreshes the registration after commits (or on a schedule). Works with today's v0 layout (D17), at the cost of a refresh step.
- **Setup in one command:** `timelake snowflake connect` creates the external volume, catalog integration and tables (or prints the SQL for a DBA to run), with least-privilege IAM for the bucket.
- **Time-series helpers in Snowflake:** a published set of SQL views and UDFs that map the function library ([spec 28](0028-function-library.md)) onto Snowflake built-ins (`TIME_SLICE`, `ASOF JOIN`, window functions), like the DuckDB macro pack.
- **Proof:** a benchmark of the same IIoT workload ([spec 45](0045-iiot-benchmark.md)) as a native Snowflake table, as naive Iceberg in Snowflake, and as Timelake tables in Snowflake, reporting latency and credits per query plus monthly storage cost.

## Open questions and issues

- [#115](https://github.com/polythings/timelake/issues/115) Question: what Snowflake reads from external Iceberg catalogs

## History

- 2026-10-13: planned (F5, SPEC.md §21.5)
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: was spec 40; specs regrouped one per feature, with milestones on the roadmap.
