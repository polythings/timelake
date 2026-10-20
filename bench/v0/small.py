r"""A small Timelake benchmark for any S3-compatible store, with no proxy.

The first N five-minute windows of TSBS IoT are appended as commits (two per window), then four queries run, each once
in a fresh `Lake` (cold) and twice more (warm). Small enough for free tiers (100 commits: about 1,000 writes, under
1,000 reads, 35 MB). Prints one JSON line.

    BENCH_URL=s3://bucket/run-1 BENCH_OPTIONS='{"aws_endpoint": "https://t3.storage.dev", "aws_region": "auto"}' \
        uv run python v0/small.py --label tigris --windows 50

Credentials come from the usual environment variables (`AWS_ACCESS_KEY_ID`, ...). `BENCH_URL` must be a new prefix,
unless `--queries-only` reruns the queries on a warehouse an earlier run wrote (reads only, so it is cheap to repeat);
`--cold-runs N` then reports the median of N first runs, each in a fresh `Lake`.
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
import statistics
import time

import numpy as np
import pyarrow.parquet as pq

import timelake
from timebench import DATA, TSBS_TIME_SERIES
from timebench.workloads.tsbs import by_window, windows

QUERIES = {
    "one-truck-latest": "SELECT time, latitude, longitude FROM readings WHERE tags_id = 1 ORDER BY time DESC LIMIT 1",
    "last-loc": """
        SELECT t.name, t.driver, r.longitude, r.latitude FROM tags t
        JOIN (SELECT tags_id, longitude, latitude, ROW_NUMBER() OVER (PARTITION BY tags_id ORDER BY time DESC) AS rn
              FROM readings) r ON r.tags_id = t.id AND r.rn = 1
        WHERE t.name IS NOT NULL AND t.fleet = 'South'""",
    "one-truck-history": "SELECT time, velocity FROM readings WHERE tags_id = 1 ORDER BY time",
    "fleet-avg-velocity": "SELECT tags_id, avg(velocity) FROM readings GROUP BY tags_id",
}


def ingest(url: str, options: dict[str, str], properties: dict[str, str], n_windows: int) -> list[float]:
    """Appends the first `n_windows` windows, two commits each; returns each commit's latency in seconds."""
    lake = timelake.Lake(url, storage_options=options)
    source = {n: pq.read_table(DATA / "tsbs" / "parquet" / f"{n}.parquet") for n in ("tags", *TSBS_TIME_SERIES)}
    lake.create_table("tsbs.tags", source["tags"].schema, properties=properties).append(source["tags"])
    tables = {
        n: lake.create_table(
            f"tsbs.{n}",
            source[n].schema,
            time_column="time",
            series_key=["tags_id"],
            dedup=False,
            properties=properties,
        )
        for n in TSBS_TIME_SERIES
    }
    latencies: list[float] = []
    arrivals = by_window({n: iter(windows(source[n], 5)) for n in TSBS_TIME_SERIES})
    for arrival in itertools.islice(arrivals, n_windows):
        for name, batch in arrival:
            started = time.perf_counter()
            tables[name].append(batch)
            latencies.append(time.perf_counter() - started)
    return latencies


def main() -> None:
    """Appends (unless `--queries-only`), queries, and prints the timings as JSON."""
    parser = argparse.ArgumentParser(description="Small Timelake benchmark for an S3-compatible store.")
    parser.add_argument("--label", required=True, help="name of the store in the output")
    parser.add_argument("--windows", type=int, default=50, help="five-minute windows to append (two commits each)")
    parser.add_argument("--head-format", help="timelake.head-format of the tables (none: no head object)")
    parser.add_argument("--queries-only", action="store_true", help="query an existing warehouse at BENCH_URL")
    parser.add_argument("--cold-runs", type=int, default=1, help="first runs per query, each in a fresh Lake")
    args = parser.parse_args()
    url, options = os.environ["BENCH_URL"], json.loads(os.environ.get("BENCH_OPTIONS", "{}"))
    properties = {"timelake.head-format": args.head_format} if args.head_format else {}

    latencies: list[float] = []
    if not args.queries_only:
        latencies = ingest(url, options, properties, args.windows)
        # The last commits' version hints and heads are written in the background; let them land, as
        # they would before a reader in another process came along.
        time.sleep(2)

    queries: dict[str, dict[str, int]] = {}
    for name, sql in QUERIES.items():
        first, warm = [], []
        for _ in range(args.cold_runs):
            reader = timelake.Lake(url, storage_options=options)
            times = []
            for _ in range(3):
                started = time.perf_counter()
                reader.sql(sql, namespace="tsbs")
                times.append(time.perf_counter() - started)
            first.append(times[0])
            warm.append(min(times[1:]))
        queries[name] = {"first_ms": round(statistics.median(first) * 1000), "best_ms": round(min(warm) * 1000)}
    result: dict[str, object] = {"label": args.label, "head_format": args.head_format or "default"}
    if latencies:
        result |= {
            "commits": len(latencies),
            "commit_ms_p50": round(statistics.median(latencies) * 1000),
            "commit_ms_p95": round(float(np.percentile(latencies, 95)) * 1000),
        }
    if args.cold_runs > 1:
        result["cold_runs"] = args.cold_runs
    print(json.dumps(result | {"queries": queries}))


if __name__ == "__main__":
    main()
