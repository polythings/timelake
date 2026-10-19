# Benchmark queries

| Suite | Source | License |
|---|---|---|
| `tsbs_iot` | Ported from [TSBS](https://github.com/timescale/tsbs) (`cmd/tsbs_generate_queries/databases/timescaledb/iot.go`); porting rules in the file's header | MIT, [LICENSE-tsbs.txt](tsbs_iot/LICENSE-tsbs.txt) |
| `tpch` | [TPC-H](https://www.tpc.org/tpch/) queries 1–22 as shipped in [DuckDB](https://github.com/duckdb/duckdb)'s `tpch` extension | MIT, [LICENSE-duckdb.txt](tpch/LICENSE-duckdb.txt). Results are not TPC-H results. |
| `tsbs_devops` | Ported from [TSBS](https://github.com/timescale/tsbs) (`cmd/tsbs_generate_queries/databases/timescaledb/devops.go`): the 15 DevOps query types on the cpu-only data; porting rules in the file's header | MIT, [LICENSE-tsbs.txt](tsbs_devops/LICENSE-tsbs.txt) |
| `iot_point` | Timelake | Apache-2.0 |
| `iiot` | Timelake: the industrial IoT workload ([spec 45](../../spec/0045-iiot-benchmark.md)); rules in the file's header | Apache-2.0 |

ClickBench's SQL isn't kept here: its license (CC BY-NC-SA 4.0) doesn't fit an Apache-2.0 repository. `timebench.queries`
downloads it from [ClickBench](https://github.com/ClickHouse/ClickBench) at a pinned commit (`CLICKBENCH_COMMIT`) into
`data/clickbench/sql` on first use.
