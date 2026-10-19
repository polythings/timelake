# Parquet vs Vortex

> **Run:** 2026-10-11, same machine and data as [BASELINE.md](BASELINE.md) and [OBJECT_STORAGE.md](OBJECT_STORAGE.md).
> **Engines:** (1) DuckDB 1.5.6 for both formats; (2) DataFusion 55.2 + `vortex-datafusion` 0.88 in Rust (section below).
> **First engine detail:** DuckDB 1.5.6. Parquet via `read_parquet`; Vortex via DuckDB's core `vortex` extension (build `b86c6ab`; Python `vortex-data` is at 0.88, pre-1.0).
> **Files:** Vortex files were written by DuckDB `COPY … (FORMAT vortex)` from the same Parquet data, in the same row order. The TSBS "sorted" layout (by `tags_id, time`, i.e. Timelake's layout) was written by DuckDB in both formats.
> **Correctness:** **0 row-count mismatches** across 181 query pairs.
> **Metrics:** First = first run in a fresh process. Best = best of 3 repeats. Values are geometric means in seconds; ratio < 1 means Vortex is faster.

## Query speed

| Workload | Queries | Parquet first | Vortex first | ratio | Parquet best | Vortex best | ratio | Vortex faster (best) | requests/query (first) P / V | MB/query (first) P / V |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TPC-H SF10, local | 22 | 0.331 | 0.215 | **0.65×** | 0.237 | 0.139 | **0.59×** | 21/22 | | |
| ClickBench 10%, local | 43 | 0.096 | 0.074 | **0.78×** | 0.039 | 0.020 | **0.52×** | 42/43 | | |
| TSBS IoT, local, as generated | 12 | 0.128 | 0.097 | **0.76×** | 0.068 | 0.043 | **0.64×** | 10/12 | | |
| TSBS IoT, local, sorted | 12 | 0.104 | 0.091 | **0.87×** | 0.055 | 0.043 | **0.77×** | 10/12 | | |
| IoT single-device, local, as generated | 5 | 0.076 | 0.047 | **0.62×** | 0.025 | 0.007 | **0.28×** | 5/5 | | |
| IoT single-device, local, sorted | 5 | 0.054 | 0.043 | **0.80×** | 0.014 | 0.004 | **0.29×** | 4/5 | | |
| TPC-H SF10, RustFS 20 ms | 22 | 1.192 | 3.634 | 3.05× | 0.311 | 3.389 | 10.91× | 0/22 | 249 / 472 | 387 / 504 |
| ClickBench 10%, RustFS 20 ms | 43 | 0.383 | 0.701 | 1.83× | 0.079 | 0.510 | 6.48× | 1/43 | 55 / 126 | 107 / 93 |
| TSBS IoT, RustFS 20 ms, sorted | 12 | 0.348 | 0.441 | 1.27× | 0.115 | 0.350 | 3.05× | 0/12 | 30 / 27 | 28 / 39 |
| IoT single-device, RustFS 20 ms, sorted | 5 | 0.222 | 0.301 | 1.36× | 0.041 | 0.147 | 3.61× | 0/5 | 12 / 22 | 11 / 13 |

## Size and write cost

| Dataset | Parquet (zstd) MB | Vortex MB | Vortex ÷ Parquet | Vortex write time |
|---|---:|---:|---:|---:|
| TPC-H SF10 | 2,317 | 2,760 | 1.19× (larger) | 82 s |
| ClickBench 10% | 1,487 | 1,233 | **0.83×** | 57 s |
| TSBS as generated¹ | 251 | 190 | **0.76×** | 4.9 s |
| TSBS sorted by (truck, time), both written by DuckDB | 195 | 224 | 1.15× (larger) | 4.7 s vs 1.9 s Parquet |

¹ The as-generated Parquet was written by PyArrow with different defaults, so the sorted row is the fairer comparison.

## Findings

1. **On local disk Vortex is clearly faster.**
   - Geomeans are 13–48% lower on TPC-H, ClickBench, and TSBS, and Vortex was faster on 92 of 99 query pairs.
   - **Single-device IoT queries were ~3.5× faster** on repeat runs (geomean 4 ms vs 14 ms in the sorted layout). That's the random-access advantage Vortex is designed for.
2. **On object storage, Vortex via DuckDB is slower today:** 1.3–3× on first runs and 3–11× on repeats. The causes are the integration, not the encoding:
   - **No caching.** The extension reads through its own Rust `object_store` client, bypassing DuckDB's in-memory file cache. It re-fetched every byte on every repeat (TPC-H: 10,296 requests on repeat runs vs 72 for Parquet).
   - **More requests.** It issued ~2× the requests of Parquet on TPC-H and ClickBench. Its I/O isn't yet tuned for high-latency storage.
3. **Compression is not a clear win.** Vortex is 17–24% smaller on ClickBench and unsorted TSBS, but 15–19% *larger* on TPC-H and on **TSBS in Timelake's sorted layout**. Writing was 2–3× slower.
4. **Ecosystem cost.** Iceberg's spec only allows Parquet, Avro, and ORC data files. Vortex tables would not be readable by Spark, Trino, Snowflake, Athena, or plain DuckDB `iceberg_scan` until Iceberg adopts pluggable file formats.

## Through DataFusion + `vortex-datafusion` (Rust)

DuckDB is only one integration. Timelake's reader will be Rust + DataFusion, so the same suites were rerun through a small Rust reader (`rust-reader/`): DataFusion 55.2 + `vortex-datafusion` 0.88, the same files, local disk and RustFS at 20 ms.
- Parquet runs twice: DataFusion defaults (`datafusion-rs`), and with Parquet filter pushdown enabled (`-pushdown`). Vortex pushes filters into the scan by default.
- Ratios are vs. default Parquet.
- Requests and MB per query are listed in engine order (Parquet / Parquet+pushdown / Vortex).
- All row counts matched.

| workload | queries | datafusion-rs first | datafusion-rs best | datafusion-rs-pushdown first | datafusion-rs-pushdown best | datafusion-rs-vortex first | datafusion-rs-vortex best | first-run requests/query | first-run MB/query | rows match |
|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|
| TPC-H SF10, local | 22/22 | 0.419 | 0.340 | 0.492 (1.17×) | 0.454 (1.34×) | 0.232 (0.55×) | 0.192 (0.57×) |  |  | ✓ |
| ClickBench 10%, local | 43/43 | 0.041 | 0.034 | 0.042 (1.02×) | 0.034 (1.01×) | 0.031 (0.76×) | 0.023 (0.67×) |  |  | ✓ |
| TSBS IoT, local, as generated | 12/12 | 0.080 | 0.067 | 0.075 (0.95×) | 0.064 (0.95×) | 0.064 (0.81×) | 0.052 (0.77×) |  |  | ✓ |
| TSBS IoT, local, sorted | 12/12 | 0.068 | 0.060 | 0.066 (0.97×) | 0.057 (0.94×) | 0.053 (0.78×) | 0.047 (0.78×) |  |  | ✓ |
| Single-device, local, as generated | 5/5 | 0.025 | 0.022 | 0.020 (0.77×) | 0.015 (0.70×) | 0.011 (0.42×) | 0.007 (0.32×) |  |  | ✓ |
| Single-device, local, sorted | 5/5 | 0.021 | 0.018 | 0.016 (0.77×) | 0.013 (0.70×) | 0.007 (0.35×) | 0.005 (0.25×) |  |  | ✓ |
| TPC-H SF10, RustFS 20 ms | 22/22 | 1.050 | 0.859 | 1.638 (1.56×) | 1.467 (1.71×) | 1.042 (0.99×) | 0.779 (0.91×) | 192 / 291 / 360 | 470 / 451 / 501 | ✓ |
| ClickBench 10%, RustFS 20 ms | 43/43 | 0.175 | 0.130 | 0.235 (1.34×) | 0.182 (1.39×) | 0.233 (1.33×) | 0.144 (1.10×) | 35 / 40 / 92 | 115 / 107 / 108 | ✓ |
| TSBS IoT, RustFS 20 ms, sorted | 12/12 | 0.291 | 0.193 | 0.362 (1.24×) | 0.282 (1.46×) | 0.339 (1.16×) | 0.213 (1.10×) | 24 / 32 / 25 | 28 / 28 / 40 | ✓ |
| Single-device, RustFS 20 ms, sorted | 5/5 | 0.118 | 0.099 | 0.186 (1.57×) | 0.136 (1.37×) | 0.175 (1.48×) | 0.124 (1.25×) | 9 / 12 / 11 | 11 / 11 / 11 | ✓ |

**What changes with the native integration:**
1. **Local disk is consistent with DuckDB.**
   - Vortex is 19–45% faster on the standard suites.
   - It's **2.4–4× faster on single-device queries**: 5 ms vs 18 ms geomean (best) in the sorted layout; 7 ms vs 21 ms on first runs.
2. **On object storage the DuckDB penalty mostly disappears.** Vortex vs. Parquet on first runs:

   | Workload | Vortex vs. Parquet |
   |---|---|
   | TPC-H | parity (0.99×) |
   | TSBS | 1.16× |
   | ClickBench | 1.33× |
   | Single-device | 1.48× |

   That's far from DuckDB's 1.3–3× first-run and 3–11× repeat-run gaps, so most of that penalty was the extension's I/O. The remaining gap is **request count**: Vortex still issues more requests (TPC-H 360 vs 192; ClickBench 92 vs 35). At 20 ms per request, that cancels its CPU advantage.
3. **Parquet filter pushdown hurts on object storage** (1.24–1.57× slower): late materialization issues extra range requests (TPC-H 291 vs 192). It helps a little locally on selective queries. Timelake should enable it per query shape, not globally.
4. **DataFusion (Rust) beat DuckDB on object-storage first runs**, Parquet vs. Parquet:

   | Workload | DataFusion | DuckDB |
   |---|---|---|
   | TPC-H | 1.05 s | 1.19 s |
   | TSBS sorted | 0.29 s | 0.35 s |
   | Single-device | 0.12 s | 0.22 s |

   It was slower on repeats, because DuckDB caches file data in memory. This supports D3 (DataFusion inside Timelake) *plus* a Timelake-owned data cache.

## Recommendation for Timelake
**Not "much better" across the board, so it doesn't justify giving up compatibility.** Confirmed through both DuckDB and the native DataFusion integration:
- On object storage, where the lake's source of truth lives, Vortex is at best at parity with Parquet today (it issues more requests).
- It *is* much better (2.4–4× on single-device queries, 20–45% elsewhere) once data is on local disk or in memory, which Timelake controls.
- **Keep Parquet as the lake's source of truth** inside Iceberg. It's compatible, more mature on object storage, and equal or smaller in our layout.
- **Use Vortex where Timelake owns the read path and the data is local:**
  - the reader's **local SSD cache** (transcode hot files to Vortex on cache fill)
  - the **hot tier** and edge nodes
  - possibly the last-value / recent-data store

  That is where the 3.5× single-device speedup lands, and where compatibility doesn't matter.
- **Keep the file-format abstraction** ([spec 5](../../spec/0005-file-layout.md)).
- **Re-test when:**
  - (a) Iceberg supports Vortex data files
  - (b) Vortex's object-store I/O adds caching and request coalescing
  - (c) Vortex's request count on object storage drops to Parquet's level. Track it with `run_rust.sh`; the harness now exists.
- **Practical constraint:** Vortex 0.88 needs DataFusion 55 / Arrow 59 and Rust 1.95, while iceberg-rust 0.10.1 is on Arrow 58. One Timelake binary needs these aligned before both can share a reader.
