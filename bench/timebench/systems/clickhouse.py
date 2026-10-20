"""ClickHouse: MergeTree tables loaded with Arrow inserts; a tuned run merges every partition (``OPTIMIZE ... FINAL``).

Any ClickHouse server works (``bench/infra/servers.sh`` starts one); set ``BENCH_CLICKHOUSE_URL`` (default
``http://default:bench@localhost:58123``). Each dataset gets a database ``bench_<dataset>`` that a run drops and
recreates. Following ClickHouse's docs: ordered by (series key, time), partitioned by month, and tables whose rows
may arrive twice use ``ReplacingMergeTree``, which removes duplicates when parts merge; until then readers query with
``FINAL``.
"""

from __future__ import annotations

import os
import urllib.parse
from typing import TYPE_CHECKING, Any, override

import pyarrow as pa

from timebench.meta import version
from timebench.systems import Runner, System, arrow_result

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

    from timebench.targets import Place, StorageStats
    from timebench.workloads import TableSpec

URL_ENV = "BENCH_CLICKHOUSE_URL"
DEFAULT_URL = "http://default:bench@localhost:58123"


def connect(database: str | None = None) -> Client:
    """A client of the benchmark server (an hour per request: maintenance merges a whole table in one)."""
    import clickhouse_connect

    url = urllib.parse.urlsplit(os.environ.get(URL_ENV, DEFAULT_URL))
    return clickhouse_connect.get_client(
        host=url.hostname or "localhost",
        port=url.port or 8123,
        username=url.username or "default",
        password=url.password or "",
        database=database or "default",
        send_receive_timeout=3600,
    )


def ch_type(field: pa.Field, spec: TableSpec, key: str) -> str:
    """The ClickHouse type of an Arrow column: nullable, but for the columns that order the table.

    Those are the time column and the series key, or a dimension table's first column (``key``). TSBS's readings
    and tags hold nulls, which a plain column would store as 0 or an empty string.
    """
    t = field.type
    if pa.types.is_timestamp(t):
        base = "DateTime64(6, 'UTC')"
    elif pa.types.is_string(t) or pa.types.is_large_string(t):
        base = "LowCardinality(String)" if field.name in spec.series_key else "String"
    else:
        types = {
            pa.int16(): "Int16",
            pa.int32(): "Int32",
            pa.int64(): "Int64",
            pa.float32(): "Float32",
            pa.float64(): "Float64",
            pa.bool_(): "Bool",
        }
        if t not in types:
            msg = f"no ClickHouse type for {t}"
            raise TypeError(msg)
        base = types[t]
    ordering = field.name in spec.series_key or field.name in (spec.time_column, key)
    return base if ordering else f"Nullable({base})"


class ClickHouse(System):
    """MergeTree tables, ReplacingMergeTree ordered by (series key, time) where rows repeat.

    Where rows repeat that is both tunings' layout: the answers are wrong without it.

    Default: ordered by time, unpartitioned, no maintenance (ClickHouse merges in the background). Tuned, as its
    docs recommend: ordered by (series key, time), partitioned by month, the workload's materialized views
    (``tunings/<workload>/clickhouse.sql``) where they stay exact, and maintenance that merges every partition.
    """

    name = "clickhouse"
    title = "ClickHouse"
    readers = ("clickhouse",)
    server = True
    tunings = ("tuned", "default")

    def __init__(self, place: Place, namespace: str) -> None:
        super().__init__(place, namespace)
        self.database = "bench_" + place.dataset.replace("-", "_")
        self._client: Client | None = None
        self.specs: dict[str, TableSpec] = {}

    @property
    def client(self) -> Client:
        """The writer's client."""
        if self._client is None:
            self._client = connect()
        return self._client

    @override
    def reset(self) -> None:
        self.place.clear()
        self.client.command(f"DROP DATABASE IF EXISTS {self.database}")
        self.client.command(f"CREATE DATABASE {self.database}")

    @override
    def create(self, spec: TableSpec, schema: pa.Schema) -> None:
        columns = ", ".join(f"{f.name} {ch_type(f, spec, schema[0].name)}" for f in schema)
        if spec.time_column:
            engine = "ReplacingMergeTree" if spec.dedup else "MergeTree"
            # Replacing removes rows with the same sort key, so it needs the series key in it.
            by_series = self.tuned or spec.dedup
            order = ", ".join([*spec.series_key, spec.time_column]) if by_series else spec.time_column
            partition = f"PARTITION BY toYYYYMM({spec.time_column}) " if self.tuned else ""
            layout = f"ENGINE = {engine} {partition}ORDER BY ({order})"
        else:
            layout = f"ENGINE = MergeTree ORDER BY {schema[0].name}"
        self.client.command(f"CREATE TABLE {self.database}.{spec.name} ({columns}) {layout}")
        self.specs[spec.name] = spec

    @override
    def tune(self, statements: list[str]) -> None:
        for sql in statements:
            self.client.command(sql.replace("{database}", self.database))

    @override
    def append(self, table: str, data: pa.Table) -> None:
        self.client.insert_arrow(f"{self.database}.{table}", data)

    @override
    def maintain(self) -> None:
        if not self.tuned:
            return
        for table in self.place.load_state()["tables"]:
            self.client.command(f"OPTIMIZE TABLE {self.database}.{table} FINAL")

    @override
    def state(self) -> dict[str, Any]:
        if not self.specs:  # a later process (maintenance): as the ingest saved it
            saved = self.place.load_state()
            return {"database": self.database, "tables": saved["tables"], "final": saved.get("final", False)}
        dedup = any(s.dedup for s in self.specs.values())
        return {"database": self.database, "tables": list(self.specs), "final": dedup}

    @override
    def stats(self) -> StorageStats:
        parts, size = self.client.query(
            f"SELECT count(), sum(bytes_on_disk) FROM system.parts WHERE database = '{self.database}' AND active"
        ).result_rows[0]
        return {"objects": None, "bytes": int(size), "data_files": int(parts), "data_bytes": int(size)}

    @override
    @classmethod
    def version(cls) -> dict[str, str]:
        from clickhouse_connect.driver.exceptions import Error

        out = {"clickhouse-connect": version("clickhouse-connect") or "?"}
        try:
            out["clickhouse"] = str(connect().command("SELECT version()"))
        except Error:  # no server: ingest records the failure
            out["clickhouse"] = "?"
        return out

    @override
    @classmethod
    def open_reader(cls, reader: str, place: Place, state: dict[str, Any]) -> Runner:
        client = connect(state["database"])
        settings = {"final": 1} if state.get("final") else {}
        return lambda sql: arrow_result(client.query_arrow(sql, settings=settings))
