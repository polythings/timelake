"""Phase 0 engine comparison (spec 44): query engines over the same static Parquet, Vortex and Iceberg files.

Workloads that write tables (ingest, maintenance, then queries per system) run with ``timebench`` instead
(``timebench/cli.py``). This runs a query suite on one engine. Each query runs in a fresh subprocess: one "first"
run, then `--runs` repeat runs. Each run fully materializes the result as Arrow. Results are appended to
results/<suite>[.<tag>].jsonl.

    python run.py --suite tpch --engine duckdb
    python run.py --suite clickbench --engine datafusion --queries 0,1,2

Environment: BENCH_MACHINE describes the machine in the records; on AWS, BENCH_S3_DIRECT=<bucket> makes plain
DuckDB read that bucket directly (instance credentials, no proxy, no request counts) to measure the proxy's
overhead, and BENCH_ICEBERG_S3_DIR names the local catalog of the Iceberg copy on S3.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import platform
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, NamedTuple

from timebench import BENCH, DATA, RESULTS, TPCH_TABLES, TSBS_TABLES
from timebench.queries import (
    DATAFUSION,
    DUCKDB,
    POLARS,
    SUITES,
    TSBS_SUITES,
    Dialect,
    apply_dialect,
    clickbench_queries,
    clickbench_sql,
    raw_queries,
)
from timebench.results import QueryRecord, append_jsonl, best_time, first_time, utc_timestamp
from timebench.s3 import BUCKET, PROXY, ProxyStats

if TYPE_CHECKING:
    from pathlib import Path

    import duckdb
    import pyarrow as pa

Runner = Callable[[str], "pa.Table"]
"""Runs one query and returns the materialized result."""

# Rust reader (rust-reader/): DataFusion 55 over Parquet or Vortex.
RUST_ENGINES = {
    "datafusion-rs": ["--format", "parquet"],
    "datafusion-rs-pushdown": ["--format", "parquet", "--pushdown"],
    "datafusion-rs-vortex": ["--format", "vortex"],
}
RUST_BIN = BENCH / "rust-reader" / "target" / "release" / "bench-reader"


DIALECTS: dict[str, Dialect] = {
    "duckdb": DUCKDB,
    "duckdb-iceberg": DUCKDB,
    "duckdb-vortex": DUCKDB,
    "datafusion": DATAFUSION,
    "polars": POLARS,
    **dict.fromkeys(RUST_ENGINES, DATAFUSION),
}
ENGINES = ["duckdb", "duckdb-iceberg", "duckdb-vortex", "datafusion", "polars", *RUST_ENGINES]


@dataclass(frozen=True)
class RunConfig:
    """What to run and where the data is."""

    suite: str
    engine: str
    storage: str
    """``local`` (bench/data) or ``s3`` (through the proxy)."""
    layout: str
    """TSBS tables: ``as-is`` (time order) or ``sorted`` by (tags_id, time)."""
    runs: int
    """Repeat runs after the first run."""
    direct_bucket: str | None = None
    iceberg_s3_dir: str = "iceberg-s3"

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> RunConfig:
        """The config from the command line and the environment."""
        return cls(
            suite=args.suite,
            engine=args.engine,
            storage=args.storage,
            layout=args.layout,
            runs=args.runs,
            direct_bucket=os.environ.get("BENCH_S3_DIRECT"),
            iceberg_s3_dir=os.environ.get("BENCH_ICEBERG_S3_DIR", "iceberg-s3"),
        )

    @property
    def s3(self) -> bool:
        """Whether the data is on S3."""
        return self.storage == "s3"

    @property
    def counted(self) -> bool:
        """Whether S3 requests go through the counting proxy."""
        return self.s3 and not self.direct_bucket

    @property
    def base(self) -> str:
        """Root of the datasets."""
        if self.s3:
            return f"s3://{self.direct_bucket or BUCKET}"
        return str(DATA)

    def tsbs_path(self, fmt: str, table: str) -> str:
        """A TSBS table file in the chosen layout."""
        layout = f"{fmt}-sorted" if self.layout == "sorted" else fmt
        return f"{self.base}/tsbs/{layout}/{table}.{fmt}"


def load_queries(suite: str, engine: str) -> list[tuple[str, str]]:
    """``(name, sql)`` of a suite in the engine's dialect."""
    if suite == "clickbench":
        datafusion_like = engine in ("datafusion", "polars", *RUST_ENGINES)
        return clickbench_queries("datafusion-partitioned" if datafusion_like else "duckdb-parquet-partitioned")
    queries = raw_queries(suite)
    if suite in TSBS_SUITES:
        return [(name, apply_dialect(sql, DIALECTS[engine])) for name, sql in queries]
    return list(queries)


# ---------------------------------------------------------------------------- engine setup (worker side)


def setup_duckdb(cfg: RunConfig, *, iceberg: bool = False, vortex: bool = False) -> Runner:
    """DuckDB over Parquet, Vortex, or (TSBS only) the Iceberg copy."""
    import duckdb

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")  # bucket by UTC days like the other engines (and avoid ICU calendar math)
    if cfg.s3:
        _duckdb_s3(con, cfg)
    if vortex:
        _duckdb_vortex_views(con, cfg)
    elif iceberg and cfg.suite in TSBS_SUITES:
        _duckdb_iceberg_views(con, cfg)
    else:
        _duckdb_parquet_views(con, cfg)
    return lambda sql: con.execute(sql).fetch_arrow_table()


def _duckdb_s3(con: duckdb.DuckDBPyConnection, cfg: RunConfig) -> None:
    con.execute("INSTALL httpfs; LOAD httpfs")
    if cfg.direct_bucket:
        con.execute("INSTALL aws; LOAD aws")
        con.execute(f"CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION '{os.environ['AWS_REGION']}')")
    else:
        con.execute(PROXY.duckdb_secret())


def _duckdb_parquet_views(con: duckdb.DuckDBPyConnection, cfg: RunConfig) -> None:
    if cfg.suite == "tpch":
        for t in TPCH_TABLES:
            con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{cfg.base}/tpch/{t}.parquet')")
    elif cfg.suite in TSBS_SUITES:
        for t in TSBS_TABLES:
            con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{cfg.tsbs_path('parquet', t)}')")
    elif cfg.suite == "clickbench":
        create = clickbench_sql("duckdb-parquet-partitioned", "create")
        # "*.parquet", not "hits_*": S3 Express rejects list prefixes that don't end in "/". Same 10 files.
        con.execute(create.replace("'hits_*.parquet'", f"'{cfg.base}/clickbench/*.parquet'"))


def _duckdb_iceberg_views(con: duckdb.DuckDBPyConnection, cfg: RunConfig) -> None:
    con.execute("INSTALL iceberg; LOAD iceberg")
    catalog = cfg.iceberg_s3_dir if cfg.s3 else "iceberg"
    metadata = json.loads((DATA / "tsbs" / catalog / "metadata.json").read_text())
    for t in TSBS_TABLES:
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM iceberg_scan('{metadata[t]}')")


def _duckdb_vortex_views(con: duckdb.DuckDBPyConnection, cfg: RunConfig) -> None:
    if cfg.s3:
        # The vortex extension reads S3 through its own Rust object_store client, not httpfs secrets.
        os.environ.update(PROXY.environment())
    con.execute("INSTALL vortex; LOAD vortex")
    if cfg.suite == "tpch":
        for t in TPCH_TABLES:
            con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_vortex('{cfg.base}/tpch/vortex/{t}.vortex')")
    elif cfg.suite in TSBS_SUITES:
        for t in TSBS_TABLES:
            con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_vortex('{cfg.tsbs_path('vortex', t)}')")
    elif cfg.suite == "clickbench":
        create = clickbench_sql("duckdb-parquet-partitioned", "create")
        create = create.replace(
            "read_parquet('hits_*.parquet', binary_as_string=True)",
            f"read_vortex('{cfg.base}/clickbench/vortex/*.vortex')",
        )
        con.execute(create)


def setup_datafusion(cfg: RunConfig) -> Runner:
    """DataFusion (Python) over Parquet."""
    from datafusion import SessionContext

    ctx = SessionContext()
    if cfg.s3:
        ctx.register_object_store("s3://", PROXY.datafusion_store(), BUCKET)
    if cfg.suite == "tpch":
        for t in TPCH_TABLES:
            ctx.register_parquet(t, f"{cfg.base}/tpch/{t}.parquet")
    elif cfg.suite in TSBS_SUITES:
        for t in TSBS_TABLES:
            ctx.register_parquet(t, cfg.tsbs_path("parquet", t))
    elif cfg.suite == "clickbench":
        create = clickbench_sql("datafusion-partitioned", "create")
        create = create.replace("LOCATION 'partitioned'", f"LOCATION '{cfg.base}/clickbench/'")
        for statement in filter(str.strip, create.split(";")):
            ctx.sql(statement).collect()
    return lambda sql: ctx.sql(sql).to_arrow_table()


def setup_polars(cfg: RunConfig) -> Runner:
    """Polars SQL over lazy Parquet scans."""
    import polars as pl

    options = PROXY.polars_options() if cfg.s3 else None
    ctx = pl.SQLContext()
    if cfg.suite == "tpch":
        for t in TPCH_TABLES:
            ctx.register(t, pl.scan_parquet(f"{cfg.base}/tpch/{t}.parquet", storage_options=options))
    elif cfg.suite in TSBS_SUITES:
        for t in TSBS_TABLES:
            lf = pl.scan_parquet(cfg.tsbs_path("parquet", t), storage_options=options)
            if t != "tags":
                lf = lf.with_columns(pl.col("time").dt.replace_time_zone(None))
            ctx.register(t, lf)
    elif cfg.suite == "clickbench":
        lf = pl.scan_parquet(f"{cfg.base}/clickbench/*.parquet", storage_options=options)
        casts = [pl.col(c).cast(pl.Utf8) for c, dtype in lf.collect_schema().items() if dtype == pl.Binary]
        lf = lf.with_columns(*casts, pl.col("EventDate").cast(pl.Int32).cast(pl.Date))
        ctx.register("hits", lf)
    return lambda sql: ctx.execute(sql, eager=True).to_arrow()


PYTHON_ENGINES: dict[str, Callable[[RunConfig], Runner]] = {
    "duckdb": setup_duckdb,
    "duckdb-iceberg": lambda cfg: setup_duckdb(cfg, iceberg=True),
    "duckdb-vortex": lambda cfg: setup_duckdb(cfg, vortex=True),
    "datafusion": setup_datafusion,
    "polars": setup_polars,
}
"""Engines that run in a Python worker process (the others are separate binaries)."""


def request_kinds(before: ProxyStats, after: ProxyStats) -> dict[str, int]:
    """Requests by kind between two proxy snapshots (kinds that didn't change are left out)."""
    old = before.get("kinds", {})
    return {k: n - old.get(k, 0) for k, n in after.get("kinds", {}).items() if n != old.get(k, 0)}


def worker(cfg: RunConfig, name: str) -> None:
    """Runs one query ``cfg.runs + 1`` times and prints the measurements as one JSON line."""
    sql = dict(load_queries(cfg.suite, cfg.engine))[name]
    setup_start = time.perf_counter()
    run = PYTHON_ENGINES[cfg.engine](cfg)
    setup_s = time.perf_counter() - setup_start
    times: list[float] = []
    requests: list[int] = []
    nbytes: list[int] = []
    kinds: list[dict[str, int]] = []
    rows = None
    for _ in range(cfg.runs + 1):
        before = PROXY.stats() if cfg.counted else None
        start = time.perf_counter()
        result = run(sql)
        times.append(time.perf_counter() - start)
        rows = result.num_rows
        if before:
            after = PROXY.stats()
            requests.append(after["requests"] - before["requests"])
            nbytes.append(after["bytes"] - before["bytes"])
            if "kinds" in after:  # AWS mode: requests by kind (GET, HEAD, LIST, PUT, ...)
                kinds.append(request_kinds(before, after))
    out: QueryRecord = {"times": times, "rows": rows, "setup_s": setup_s}
    if cfg.counted:
        out.update(requests=requests, bytes=nbytes, ttfb_ms=PROXY.stats()["ttfb_ms"])
        if kinds:
            out["kinds"] = kinds
    print(json.dumps(out))


# ---------------------------------------------------------------------------- driver


def engine_version(engine: str) -> str:
    """The engine and its version, as recorded in results."""
    if engine in RUST_ENGINES:
        return "df55.2+vortex0.88"
    module = "duckdb" if engine.startswith("duckdb") else engine
    return importlib.import_module(module).__version__


class QueryCommand(NamedTuple):
    """A process that runs one query: its command line, stdin, and environment (``None``: this one's)."""

    cmd: list[str]
    stdin: str | None = None
    env: dict[str, str] | None = None


def query_command(cfg: RunConfig, name: str, sql: str) -> QueryCommand:
    """The process that runs one query.

    The Rust reader reaches S3 through ``PROXY`` as its ``AWS_*`` variables say, and counts requests on its admin
    address.
    """
    if cfg.engine in RUST_ENGINES:
        cmd = [str(RUST_BIN), "--suite", cfg.suite, "--runs", str(cfg.runs), "--storage", cfg.storage]
        cmd += ["--layout", cfg.layout, *RUST_ENGINES[cfg.engine]]
        if not cfg.s3:
            return QueryCommand(cmd, sql)
        cmd += ["--bucket", BUCKET, "--proxy-admin", str(PROXY.admin)]
        return QueryCommand(cmd, sql, {**os.environ, **PROXY.environment()})
    common = ["--suite", cfg.suite, "--engine", cfg.engine, "--runs", str(cfg.runs), "--storage", cfg.storage]
    return QueryCommand([sys.executable, __file__, *common, "--layout", cfg.layout, "--worker", name])


def measure(command: QueryCommand, timeout: int) -> QueryRecord:
    """Runs one query process; its measurements plus ``status`` (and ``error``)."""
    cmd, stdin, env = command
    try:
        # A failing query is a result to record, not a reason to stop: no check=True.
        proc = subprocess.run(cmd, input=stdin, env=env, capture_output=True, text=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired:
        return {"status": "timeout"}
    if proc.returncode == 0:
        measured: QueryRecord = json.loads(proc.stdout.strip().splitlines()[-1])
        return {**measured, "status": "ok"}
    lines = proc.stderr.strip().splitlines() or ["?"]
    error = next((line for line in reversed(lines) if "Error" in line or "Exception" in line), lines[-1])
    return {"status": "error", "error": error[:300]}


def summary_line(r: QueryRecord) -> str:
    """One line per query for the console."""
    prefix = f"{r['engine']:15} {r['suite']:10} {r['query']:36}"
    if r["status"] != "ok":
        return f"{prefix} {r['status'].upper()}: {r.get('error', '')[:120]}"
    reqs = f"  reqs {r['requests'][0]}/{min(r['requests'])}" if "requests" in r else ""
    return f"{prefix} first {first_time(r):8.3f}s  best {best_time(r):8.3f}s  rows {r['rows']}{reqs}"


def drive(cfg: RunConfig, queries: list[str], timeout: int, out: Path) -> None:
    """Runs each query in its own process and appends one record per query to ``out``."""
    meta: QueryRecord = {
        "suite": cfg.suite,
        "engine": cfg.engine,
        "version": engine_version(cfg.engine),
        "machine": os.environ.get("BENCH_MACHINE") or f"{platform.machine()} {os.cpu_count()} cores",
        "storage": cfg.storage,
        "layout": cfg.layout,
        "ts": utc_timestamp(),
    }
    sqls = dict(load_queries(cfg.suite, cfg.engine))
    for name in queries:
        record: QueryRecord = {**meta, "query": name}
        if cfg.direct_bucket:
            record["direct"] = True
        before = PROXY.stats() if cfg.counted else None
        record.update(measure(query_command(cfg, name, sqls[name]), timeout))
        # Requests by kind over all runs, for engines that don't report them per run (Rust, Timelake CLI).
        if before and "kinds" in before and (total := request_kinds(before, PROXY.stats())):
            record["kinds_total"] = total
        append_jsonl(out, record)
        print(summary_line(record))


def parse_args() -> argparse.Namespace:
    """The command line."""
    p = argparse.ArgumentParser()
    p.add_argument("--suite", required=True, choices=SUITES)
    p.add_argument("--engine", required=True, choices=ENGINES)
    p.add_argument("--layout", default="as-is", choices=["as-is", "sorted"], help="TSBS tables only")
    p.add_argument("--runs", type=int, default=3, help="repeat runs after the first run")
    p.add_argument("--timeout", type=int, default=300, help="seconds per query (all runs)")
    p.add_argument("--queries", help="comma-separated query names or ClickBench indexes")
    p.add_argument("--storage", default="local", choices=["local", "s3"], help="s3 = local RustFS via tools/s3proxy")
    p.add_argument("--tag", help="results go to results/<suite>.<tag>.jsonl")
    p.add_argument("--worker", help=argparse.SUPPRESS)
    return p.parse_args()


def main() -> None:
    """Runs the selected queries (or, with --worker, one query in this process)."""
    args = parse_args()
    cfg = RunConfig.from_args(args)
    if args.worker:
        worker(cfg, args.worker)
        return
    queries = [name for name, _ in load_queries(cfg.suite, cfg.engine)]
    if args.queries:
        wanted = {q if not q.isdigit() else f"q{int(q):02d}" for q in args.queries.split(",")}
        queries = [q for q in queries if q in wanted]
    out = RESULTS / (f"{cfg.suite}.{args.tag}.jsonl" if args.tag else f"{cfg.suite}.jsonl")
    drive(cfg, queries, args.timeout, out)


if __name__ == "__main__":
    main()
