"""QuestDB: WAL tables loaded over QWP (its Python client), read over PGWire.

Any QuestDB server works (``infra/servers.sh`` starts one; ``BENCH_QUESTDB_HTTP`` and ``BENCH_QUESTDB_PG`` name
another). It has no schemas, so the server holds one dataset at a time: a run drops and recreates the workload's
tables. Its open-source edition keeps data on local disk; tiering to object storage is Enterprise-only.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, LiteralString, cast, override

import pyarrow as pa

from timebench.meta import version
from timebench.systems import (
    UNKNOWN,
    Runner,
    System,
    pg_execute,
    pg_runner,
    server_storage,
    server_versions,
)
from timebench.targets import Tuning

if TYPE_CHECKING:
    import psycopg
    import questdb
    from psycopg.abc import Params

    from timebench.config import BenchConfig
    from timebench.targets import Place, StorageStats
    from timebench.workloads import TableSpec

SETTLE_TIMEOUT_S = 600


def connect(config: BenchConfig) -> psycopg.Connection:
    """A PGWire connection (QuestDB's defaults: user admin, password quest, database qdb)."""
    import psycopg

    url = f"postgresql://admin:quest@{config.questdb_pg}/qdb"
    return psycopg.connect(url, autocommit=True, prepare_threshold=None)


def qdb_type(field: pa.Field, spec: TableSpec) -> str:
    """The QuestDB type of an Arrow column: series-key strings are symbols."""
    t = field.type
    if pa.types.is_timestamp(t):
        return "TIMESTAMP"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "SYMBOL" if field.name in spec.series_key else "VARCHAR"
    types = {pa.int16(): "SHORT", pa.int32(): "INT", pa.int64(): "LONG", pa.float64(): "DOUBLE", pa.bool_(): "BOOLEAN"}
    if t not in types:
        msg = f"no QuestDB type for {t}"
        raise TypeError(msg)
    return types[t]


class QuestDB(System):
    """Day-partitioned WAL tables with a designated timestamp; dimension tables without one.

    Tables whose rows may arrive twice deduplicate on write (``DEDUP UPSERT KEYS``) in both tunings: the answers are
    wrong without it. WAL tables apply commits asynchronously: the probes measure that lag, and a checkpoint waits
    until every commit is applied (``settle``). Default: the series key a plain ``SYMBOL``. Tuned, as QuestDB's docs
    recommend: the symbol indexed, and the workload's materialized views (``tunings/<workload>/questdb.sql``), which
    a run waits for whenever it waits for the tables.
    """

    name = "questdb"
    title = "QuestDB"
    readers = ("questdb",)
    server = True
    tunings = (Tuning.TUNED, Tuning.DEFAULT)

    def __init__(self, place: Place, namespace: str) -> None:
        super().__init__(place, namespace)
        self._conn: psycopg.Connection | None = None
        self._db: questdb.QuestDB | None = None

    @property
    def conn(self) -> psycopg.Connection:
        """The writer's PGWire connection (DDL, dimension rows, stats)."""
        if self._conn is None:
            self._conn = connect(self.place.config)
        return self._conn

    @property
    def db(self) -> questdb.QuestDB:
        """The writer's QWP client, kept across commits as the other databases keep theirs (until ``settle``)."""
        if self._db is None:
            import questdb

            self._db = questdb.connect(f"ws::addr={self.place.config.questdb_http};")
        return self._db

    def close_db(self) -> None:
        """Closes the QWP client, if open."""
        if self._db is not None:
            self._db.close()
            self._db = None

    def execute(self, sql: str, params: Params | None = None) -> list[tuple[Any, ...]]:
        """Runs a statement on the writer's connection."""
        return pg_execute(self.conn, sql, params)

    @override
    def reset(self) -> None:
        self.place.clear()
        for spec in self.place_tables():
            self.execute(f"DROP TABLE IF EXISTS {spec}")

    def place_tables(self) -> list[str]:
        """The tables an earlier run of this dataset left, if any."""
        try:
            return list(self.place.load_state().get("tables", []))
        except FileNotFoundError:
            return []

    @override
    def create(self, spec: TableSpec, schema: pa.Schema) -> None:
        super().create(spec, schema)
        self.execute(f"DROP TABLE IF EXISTS {spec.name}")
        columns = ", ".join(
            f"{f.name} {qdb_type(f, spec)}{' INDEX' if self.tuned and f.name in spec.series_key else ''}"
            for f in schema
        )
        sql = f"CREATE TABLE {spec.name} ({columns})"
        if spec.time_column:
            sql += f" TIMESTAMP({spec.time_column}) PARTITION BY DAY WAL"
            if spec.dedup:
                sql += f" DEDUP UPSERT KEYS({', '.join([spec.time_column, *spec.series_key])})"
        self.execute(sql)

    @override
    def run_statement(self, sql: str) -> None:
        self.execute(sql)

    @override
    def append(self, table: str, data: pa.Table) -> None:
        spec = self.specs[table]
        if not spec.time_column:
            # Dimension rows over PGWire: QWP needs a designated timestamp.
            names = ", ".join(data.schema.names)
            marks = ", ".join(["%s"] * data.num_columns)
            with self.conn.cursor() as cur:
                rows = list(zip(*(c.to_pylist() for c in data.columns), strict=True))
                cur.executemany(cast("LiteralString", f"INSERT INTO {table} ({names}) VALUES ({marks})"), rows)
            return
        # The client's columnar path takes each column as one buffer: a batch made of several chunks (IIoT's
        # arrivals) has its string columns in pieces, which it rejects (#154). It returns once the frame is committed.
        # Through pandas rather than Arrow: the client stores the same rows, and commits faster that way (#161).
        frame = data.combine_chunks().to_pandas()
        self.db.dataframe(frame, table_name=table, symbols=list(spec.series_key), at=spec.time_column)

    @override
    def settle(self) -> float:
        self.close_db()
        start = time.monotonic()
        while time.monotonic() < start + SETTLE_TIMEOUT_S:
            pending = self.execute("SELECT count() FROM wal_tables() WHERE writerTxn < sequencerTxn")
            # A materialized view refreshes after its base table's commits are applied.
            views = self.execute(
                "SELECT count() FROM materialized_views() WHERE refresh_base_table_txn < base_table_txn"
            )
            if (not pending or pending[0][0] == 0) and (not views or views[0][0] == 0):
                return time.monotonic() - start
            time.sleep(0.1)
        msg = f"QuestDB didn't apply its WAL within {SETTLE_TIMEOUT_S}s"
        raise TimeoutError(msg)

    @override
    def maintain(self) -> None:
        if self._conn is not None:  # the server closes connections left idle since ingest: start a new one
            self._conn.close()
            self._conn = None
        self.close_db()
        self.settle()  # nothing to compact: deduplicated on write, partitions written in order

    @override
    def state(self) -> dict[str, Any]:
        return {"tables": list(self.specs) or self.place_tables()}

    @override
    def stats(self) -> StorageStats:
        size = 0
        for table in self.state()["tables"]:
            rows = self.execute(f"SELECT sum(diskSize) FROM table_partitions('{table}')")
            size += int(rows[0][0] or 0) if rows else 0
        return server_storage(size)

    @override
    @classmethod
    def version(cls, place: Place) -> dict[str, str]:
        import psycopg

        def ask() -> dict[str, str]:
            with connect(place.config) as conn:
                row = conn.execute("SELECT build()").fetchone()
            return {
                "questdb": str(row[0]).split(",")[0].removeprefix("Build Information: QuestDB ") if row else UNKNOWN
            }

        return {"questdb-client": version("questdb") or UNKNOWN, **server_versions(ask, ("questdb",), (psycopg.Error,))}

    @override
    @classmethod
    def open_reader(cls, reader: str, place: Place, state: dict[str, Any]) -> Runner:
        return pg_runner(connect(place.config))
