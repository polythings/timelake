---
number: 35
title: Multitenancy
area: Integrations
status: planned
updated: 2026-10-16
versions: []
related: [18, 40]
issues: [141, 142]
---

# 35. Multitenancy

**Planned** · updated 2026-10-16 · milestones: [Any engine can attach](milestones/any-engine.md), [Technical preview](milestones/technical-preview.md)
· related: [spec 18](0018-gdpr-erasure.md), [spec 40](0040-agent-access.md)

## Summary

Documented patterns for many tenants: a shared table with a tenant column, a namespace per tenant, or a bucket per tenant, with their isolation and cost, plus credential vending and per-tenant retention, rollups and quotas.

## Design

Documented patterns with trade-offs and examples:
| Pattern | Isolation | Cost / ops | When |
|---------|-----------|-----------|------|
| Shared table, `tenant` partition column | Row-level (enforced by catalog/engine policies) | Cheapest | Many small tenants |
| Namespace per tenant | Catalog-level ACLs | Low | Tens to thousands of tenants |
| Warehouse/bucket per tenant | IAM + per-tenant KMS keys | Higher | Regulated / enterprise tenants |

Plus examples for credential vending via the REST catalog, per-tenant retention and rollups, and per-tenant ingestion quotas.

## Open questions and issues

- [#141](https://github.com/polythings/timelake/issues/141) Docs: a setup guide per object store
- [#142](https://github.com/polythings/timelake/issues/142) Docs: recipes for common deployments

## History

- 2026-10-11: specified (SPEC.md §14)
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: was spec 29; specs regrouped one per feature, with milestones on the roadmap.
