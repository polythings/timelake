---
title: License and open core
updated: 2026-10-12
---

# License and open core

## Summary

Everything in this repository is Apache 2.0 (decided). What is open source and what could be premium (industrial historian connectors, a managed control plane, enterprise features) is a draft.

## Design

| Open source | Premium (candidates) |
|-------------|----------------------|
| Table conventions, writer, catalog, maintenance jobs | Industrial historian connectors (AVEVA PI, Ignition, PHD, IP.21), OPC-UA at scale |
| Ingestion pipeline + built-in rules | Managed/hosted control plane, UI, alert management |
| Rollups + function library, DuckDB extension, Python library | Enterprise SSO/RBAC, compliance reporting (GDPR erasure certificates as a service) |
| Import: CSV, Parquet, Iceberg/S3 Tables, Delta, TimescaleDB/Postgres, InfluxDB | Support / SLAs |

The principle: everything needed to run the lake well is open; integrations with proprietary industrial systems and managed operations are premium. The open-source part is Apache 2.0 (D7); premium components would live outside this repository.

## Decisions

| ID | Decision | Status |
|---|---|---|
| D7 | License: **Apache License 2.0** for everything in this repository. Apache 2.0's explicit patent grant suits infrastructure that companies embed, and it matches Iceberg, Arrow, DataFusion and Parquet. The repository is public (2026-10-12). AGPLv3 + CLA and FSL were considered to keep cloud vendors from hosting it, and rejected: an open-format, no-lock-in project wins by adoption (the PostgreSQL/Iceberg/DuckDB path), late restrictions are answered by forks (Valkey, OpenSearch), and an API can be reimplemented regardless of license (DocumentDB). Protection comes from the trademark, being the reference implementation, and a hosted service plus premium add-ons ([license](license.md)). | Accepted |
| D12 | Open core: the lake, ingestion, rollups, import of open formats/DBs, and the DuckDB extension are open source. Industrial historian connectors are a **premium** offering ([license](license.md)). | Proposed |

## Open questions and issues

None open.

## History

- 2026-10-12: D7, Apache 2.0, repository public
- 2026-10-13: D12 drafted (SPEC.md §20)
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: was spec 8; a background page since the specs were regrouped one per feature.
