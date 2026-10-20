"""TimescaleDB: hypertables loaded with ``COPY`` per commit; maintenance converts every chunk to the columnstore.

Any TimescaleDB server works: a local container, one on the benchmark instance, or Tiger Cloud. Set
``BENCH_TIMESCALEDB_DSN`` (never a production database: each dataset gets a schema ``bench_<dataset>`` that a
run drops and recreates). The server keeps its data on its own disk, so the run's target doesn't apply, and a
query's "first run" is warm on the server's side (its buffer cache), unlike the lake readers' fresh processes.
"""

from __future__ import annotations

import io
import os
from typing import TYPE_CHECKING, Any, LiteralString, cast, override

import pyarrow as pa
import pyarrow.csv as pacsv

from timebench.systems import Result, Runner, System, rows_result

if TYPE_CHECKING:
    from collections.abc import Iterator

    import psycopg

    from timebench.targets import Place, StorageStats
    from timebench.workloads import TableSpec

DSN_ENV = "BENCH_TIMESCALEDB_DSN"
CHUNK_INTERVAL = "12 hours"
"""Hypertable chunk size: the TSBS loader's default."""


def connect(schema: str | None = None) -> psycopg.Connection:
    """An autocommit connection in UTC, with the dataset's schema on the search path."""
    import psycopg
    from psycopg import sql

    dsn = os.environ.get(DSN_ENV)
    if not dsn:
        msg = f"set {DSN_ENV} to a TimescaleDB server used only for benchmarks"
        raise SystemExit(msg)
    conn = psycopg.connect(dsn, autocommit=True)
    conn.execute("SET TimeZone = 'UTC'")
    if schema:
        conn.execute(sql.SQL("SET search_path = {}, public").format(sql.Identifier(schema)))
    return conn


def pg_type(dtype: pa.DataType) -> str:
    """The PostgreSQL type of an Arrow column."""
    if pa.types.is_timestamp(dtype):
        return "timestamptz" if dtype.tz else "timestamp"
    types = {
        pa.int16(): "smallint",
        pa.int32(): "integer",
        pa.int64(): "bigint",
        pa.float32(): "real",
        pa.float64(): "double precision",
        pa.bool_(): "boolean",
        pa.string(): "text",
        pa.large_string(): "text",
        pa.date32(): "date",
    }
    if dtype not in types:
        msg = f"no PostgreSQL type for {dtype}"
        raise TypeError(msg)
    return types[dtype]


def csv_chunks(data: pa.Table, batch_rows: int = 100_000) -> Iterator[bytes]:
    """Rows as headerless CSV for ``COPY ... (FORMAT csv)``: nulls unquoted and empty, strings quoted."""
    options = pacsv.WriteOptions(include_header=False)
    for batch in data.to_batches(max_chunksize=batch_rows):
        buffer = io.BytesIO()
        pacsv.write_csv(batch, buffer, options)
        yield buffer.getvalue()


class TimescaleDB(System):
    """Hypertables partitioned by time in 12-hour chunks.

    Tables whose rows may arrive twice (``TableSpec.dedup``) get a unique index on (series key, time), and rows are
    inserted from a staging table with ``ON CONFLICT DO NOTHING``, in both tunings: without it the answers are wrong.

    Default: hypertables as ``create_hypertable`` makes them (an index on time), row store, maintenance an
    ``ANALYZE``. Tuned: an index on (series key, time DESC) like TSBS's loader, the workload's continuous aggregates
    (``tunings/<workload>/timescaledb.sql``), and maintenance that converts every chunk to the columnstore (segmented
    by the series key) and refreshes the aggregates.
    """

    name = "timescaledb"
    title = "TimescaleDB"
    readers = ("postgres",)
    server = True
    tunings = ("tuned", "default")

    def __init__(self, place: Place, namespace: str) -> None:
        super().__init__(place, namespace)
        self.schema = "bench_" + place.dataset.replace("-", "_")
        self._conn: psycopg.Connection | None = None
        self.specs: dict[str, TableSpec] = {}

    @property
    def conn(self) -> psycopg.Connection:
        """The writer's connection."""
        if self._conn is None:
            self._conn = connect(self.schema)
        return self._conn

    def execute(self, sql: str) -> list[tuple[Any, ...]]:
        """Runs a statement built from this module's own templates and the workload's names."""
        cur = self.conn.execute(cast("LiteralString", sql))
        return cur.fetchall() if cur.description else []

    @override
    def reset(self) -> None:
        self.place.clear()  # the local state file
        self.execute(f"DROP SCHEMA IF EXISTS {self.schema} CASCADE")
        self.execute(f"CREATE SCHEMA {self.schema}")

    @override
    def create(self, spec: TableSpec, schema: pa.Schema) -> None:
        columns = ", ".join(
            f"{f.name} {pg_type(f.type)}{' NOT NULL' if f.name == spec.time_column else ''}" for f in schema
        )
        self.execute(f"CREATE TABLE {spec.name} ({columns})")
        if spec.time_column:
            self.execute(
                f"SELECT create_hypertable('{spec.name}', by_range('{spec.time_column}', INTERVAL '{CHUNK_INTERVAL}'))"
            )
            if spec.dedup or self.tuned:
                unique = "UNIQUE " if spec.dedup else ""
                keys = ", ".join(spec.series_key)
                self.execute(f"CREATE {unique}INDEX ON {spec.name} ({keys}, {spec.time_column} DESC)")
        if spec.dedup:
            self.execute(f"CREATE UNLOGGED TABLE {spec.name}_staging (LIKE {spec.name})")
        self.specs[spec.name] = spec

    @override
    def tune(self, statements: list[str]) -> None:
        for sql in statements:
            self.execute(sql)

    @override
    def append(self, table: str, data: pa.Table) -> None:
        dedup = self.specs[table].dedup
        target = f"{table}_staging" if dedup else table
        with (
            self.conn.cursor() as cur,
            cur.copy(cast("LiteralString", f"COPY {target} FROM STDIN (FORMAT csv)")) as copy,
        ):
            for chunk in csv_chunks(data):
                copy.write(chunk)
        if dedup:
            # Rows that arrive twice: COPY can't skip them, so they land in a staging table first (TimescaleDB's
            # docs: a unique index and INSERT ... ON CONFLICT DO NOTHING).
            with self.conn.transaction():
                self.execute(f"INSERT INTO {table} SELECT * FROM {target} ON CONFLICT DO NOTHING")
                self.execute(f"TRUNCATE {target}")

    @override
    def maintain(self) -> None:
        if not self.tuned:
            self.execute("ANALYZE")
            return
        for table, spec in self.place.load_state()["specs"].items():
            if not spec["time_column"]:
                continue
            self.execute(
                f"ALTER TABLE {table} SET (timescaledb.enable_columnstore, "
                f"timescaledb.segmentby = '{', '.join(spec['series_key'])}', "
                f"timescaledb.orderby = '{spec['time_column']} DESC')"
            )
            for (chunk,) in self.execute(f"SELECT show_chunks('{table}')"):
                self.execute(f"CALL convert_to_columnstore('{chunk}')")
        views = self.execute(
            f"SELECT view_name FROM timescaledb_information.continuous_aggregates WHERE view_schema = '{self.schema}'"
        )
        for (view,) in views:
            self.execute(f"CALL refresh_continuous_aggregate('{self.schema}.{view}', NULL, NULL)")
        self.execute("ANALYZE")

    @override
    def state(self) -> dict[str, Any]:
        specs = {
            n: {"time_column": s.time_column, "series_key": list(s.series_key)} for n, s in self.specs.items()
        } or self.place.load_state()["specs"]
        return {"schema": self.schema, "specs": specs, "tables": {n: f"{self.schema}.{n}" for n in specs}}

    @override
    def stats(self) -> StorageStats:
        size = 0
        for table, spec in self.place.load_state()["specs"].items():
            fn = "hypertable_size" if spec["time_column"] else "pg_total_relation_size"
            size += self.execute(f"SELECT {fn}('{self.schema}.{table}')")[0][0]
        return {"objects": None, "bytes": size, "data_files": 0, "data_bytes": size}

    @override
    @classmethod
    def version(cls) -> dict[str, str]:
        if not os.environ.get(DSN_ENV):
            return {"timescaledb": "?"}
        with connect() as conn:
            row = conn.execute("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").fetchone()
            pg = conn.execute("SHOW server_version").fetchone()
        return {"timescaledb": row[0] if row else "?", "postgres": pg[0].split()[0] if pg else "?"}

    @override
    @classmethod
    def open_reader(cls, reader: str, place: Place, state: dict[str, Any]) -> Runner:
        conn = connect(state["schema"])

        def run(sql: str) -> Result:
            # The query text comes from queries/, so it is trusted SQL.
            return rows_result(conn.execute(cast("LiteralString", sql)).fetchall())

        return run
