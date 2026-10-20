"""InfluxDB 3 Core: line protocol over HTTP (``/api/v3/write_lp``), SQL over HTTP (``/api/v3/query_sql``).

Any InfluxDB 3 Core server works (``bench/infra/servers.sh`` starts two). Tuned (``BENCH_INFLUXDB_URL``, default
``http://localhost:58181``), the server runs with ``--query-file-limit`` raised: by default a query may read at most
432 Parquet files, about 3 days of data in 10-minute files, and longer ones fail. Default
(``BENCH_INFLUXDB_DEFAULTS_URL``, default ``http://localhost:58182``, server ``influxdb3-defaults``), as shipped.

Each dataset gets a database ``bench_<dataset>`` that a run deletes and recreates. Tables are created on write: the
series key columns are tags, the rest fields; InfluxDB keeps one row per series and time (the last written).
Dimension tables have no time: their rows are written at the epoch, each its own series (the first column a tag).
Writes are batched and sent by a few concurrent requests, as InfluxData's docs suggest for throughput; Core has no
compaction, so maintenance is a no-op. A write is acknowledged once the server's WAL is flushed (every second).
"""

from __future__ import annotations

import gzip
import http.client
import io
import json
import os
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, ClassVar, override

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from timebench.systems import Runner, System, arrow_result

if TYPE_CHECKING:
    from timebench.targets import Place, StorageStats
    from timebench.workloads import TableSpec

BATCH_LINES = 50_000
WRITERS = 4
"""Concurrent write requests."""


Text = pa.Array | pa.ChunkedArray | str


def join(*parts: Text, sep: str = "") -> pa.ChunkedArray:
    """Strings joined row by row (strings stand for the same value in every row); null if any part is null."""
    return pc.call_function("binary_join_element_wise", [*parts, sep])


def replace(values: Text, pattern: str, replacement: str, *, regex: bool = False) -> pa.ChunkedArray:
    """Every match of ``pattern`` replaced, row by row."""
    name = "replace_substring_regex" if regex else "replace_substring"
    return pc.call_function(name, [values], pc.ReplaceSubstringOptions(pattern, replacement))


def escape_tag(values: pa.ChunkedArray) -> pa.ChunkedArray:
    """Line protocol escaping for tag values: commas, equals signs and spaces."""
    for ch in (",", "=", " "):
        values = replace(values, ch, "\\" + ch)
    return values


def field_text(column: pa.ChunkedArray) -> pa.ChunkedArray:
    """A field's values in line protocol: integers with ``i``, floats as they are, strings quoted."""
    t = column.type
    if pa.types.is_integer(t):
        return join(column.cast(pa.string()), "i")
    if pa.types.is_floating(t):
        return column.cast(pa.string())
    if pa.types.is_boolean(t):
        return pc.call_function("if_else", [column, "true", "false"])
    quoted = replace(replace(column.cast(pa.string()), "\\", "\\\\"), '"', '\\"')
    return join('"', quoted, '"')


def line_protocol(table: str, data: pa.Table, spec: TableSpec) -> pa.ChunkedArray:
    """One line per row: ``table,<tags> <fields> <microseconds>``; null fields are left out of the line.

    The series key columns are the tags; a dimension table's first column is (one series per row, at the epoch).
    """
    keys = spec.series_key if spec.time_column else (data.schema.names[0],)
    tags = [join(f"{c}=", escape_tag(data[c].cast(pa.string()))) for c in keys]
    fields = []
    for name in data.schema.names:
        if name not in keys and name != spec.time_column:
            column = data[name]
            null = pc.call_function("is_null", [column])
            fields.append(pc.call_function("if_else", [null, "", join(f"{name}=", field_text(column))]))
    if spec.time_column:
        micros = data[spec.time_column].cast(pa.timestamp("us")).cast(pa.int64()).cast(pa.string())
    else:
        micros = pa.chunked_array([["0"] * data.num_rows], pa.string())
    field_set = join(*fields, sep=",")
    field_set = replace(replace(field_set, ",,+", ",", regex=True), "^,|,$", "", regex=True)  # the null fields
    return join(join(table, *tags, sep=","), field_set, micros, sep=" ")


class InfluxDB3(System):
    """InfluxDB 3 Core.

    Default: as shipped, tables created by the first write. Tuned: the query file limit raised (its server), and each
    time-series table created up front with a last value cache keyed by its series key, which the tuned latest-value
    queries (``[influxdb3 tuned]``) read with ``last_cache()``.
    """

    name = "influxdb3"
    title = "InfluxDB 3 Core"
    readers = ("influxdb3",)
    tunings = ("tuned", "default")
    server = True
    urls: ClassVar[dict[str, tuple[str, str]]] = {
        "tuned": ("BENCH_INFLUXDB_URL", "http://localhost:58181"),
        "default": ("BENCH_INFLUXDB_DEFAULTS_URL", "http://localhost:58182"),
    }
    """Per tuning: the variable naming the server, and its default."""

    def __init__(self, place: Place, namespace: str) -> None:
        super().__init__(place, namespace)
        self.database = "bench_" + place.dataset.replace("-", "_")
        self.specs: dict[str, TableSpec] = {}

    @property
    def url(self) -> str:
        """The server of this tuning."""
        return server_url(self.tuning)

    def request(
        self, method: str, path: str, body: bytes | None = None, headers: dict[str, str] | None = None
    ) -> bytes:
        """One HTTP request to this tuning's server."""
        return request(self.url, method, path, body, headers)

    @override
    def server_name(self) -> str:
        return self.name if self.tuned else f"{self.name}-defaults"

    @override
    def reset(self) -> None:
        self.place.clear()
        try:
            self.request("DELETE", f"/api/v3/configure/database?db={self.database}")
        except RuntimeError as e:
            if "404" not in str(e):
                raise
        body = json.dumps({"db": self.database}).encode()
        self.request("POST", "/api/v3/configure/database", body, {"Content-Type": "application/json"})

    @override
    def create(self, spec: TableSpec, schema: pa.Schema) -> None:
        self.specs[spec.name] = spec  # the default's tables are created by the first write
        if not (self.tuned and spec.time_column):
            return
        json_headers = {"Content-Type": "application/json"}
        fields = [
            {"name": f.name, "type": influx_type(f.type)}
            for f in schema
            if f.name not in spec.series_key and f.name != spec.time_column
        ]
        table = {"db": self.database, "table": spec.name, "tags": list(spec.series_key), "fields": fields}
        self.request("POST", "/api/v3/configure/table", json.dumps(table).encode(), json_headers)
        keys = list(spec.series_key)
        cache = {"db": self.database, "table": spec.name, "name": f"{spec.name}_last", "key_columns": keys}
        self.request("POST", "/api/v3/configure/last_cache", json.dumps(cache).encode(), json_headers)

    @override
    def append(self, table: str, data: pa.Table) -> None:
        lines = line_protocol(table, data, self.specs[table])
        path = f"/api/v3/write_lp?db={self.database}&precision=microsecond"
        headers = {"Content-Encoding": "gzip", "Content-Type": "text/plain; charset=utf-8"}

        def send(start: int) -> None:
            batch = lines.slice(start, BATCH_LINES).to_pylist()
            self.request("POST", path, gzip.compress(("\n".join(batch) + "\n").encode(), 1), headers)

        with ThreadPoolExecutor(WRITERS) as pool:
            list(pool.map(send, range(0, len(lines), BATCH_LINES)))

    @override
    def maintain(self) -> None:
        pass  # Core has no compaction (InfluxData's docs point to Enterprise for it)

    @override
    def state(self) -> dict[str, Any]:
        tables = list(self.specs) or self.place.load_state()["tables"]
        return {"database": self.database, "url": self.url, "tables": tables}

    @override
    def stats(self) -> StorageStats:
        sql = "SELECT count(*) AS files, sum(size_bytes) AS bytes FROM system.parquet_files"
        files = query(self.url, self.database, sql)
        n = int(files["files"][0].as_py() or 0) if files.num_rows else 0
        size = int(files["bytes"][0].as_py() or 0) if files.num_rows else 0
        return {"objects": n, "bytes": size, "data_files": n, "data_bytes": size}

    @override
    @classmethod
    def version(cls) -> dict[str, str]:
        return server_version(server_url("tuned"))

    @override
    def versions(self) -> dict[str, str]:
        return server_version(self.url)

    @override
    @classmethod
    def open_reader(cls, reader: str, place: Place, state: dict[str, Any]) -> Runner:
        return lambda sql: arrow_result(query(state["url"], state["database"], sql))


def influx_type(t: pa.DataType) -> str:
    """A field's type as InfluxDB's table API names it."""
    if pa.types.is_integer(t):
        return "int64"
    if pa.types.is_floating(t):
        return "float64"
    if pa.types.is_boolean(t):
        return "bool"
    return "utf8"


def server_url(tuning: str) -> str:
    """The server of a tuning: its variable, else the port ``infra/servers.sh`` gives it."""
    env, default = InfluxDB3.urls[tuning]
    return os.environ.get(env, default).rstrip("/")


def server_version(url: str) -> dict[str, str]:
    """The server's version, or ``?`` when it doesn't answer."""
    try:
        info = json.loads(request(url, "GET", "/ping"))
    except (OSError, RuntimeError):
        return {"influxdb3": "?"}
    return {"influxdb3": str(info.get("version", "?"))}


def request(
    base: str, method: str, path: str, body: bytes | None = None, headers: dict[str, str] | None = None
) -> bytes:
    """One HTTP request to a server; raises with the server's message on an error status."""
    url = urllib.parse.urlsplit(base)
    conn = http.client.HTTPConnection(url.hostname or "localhost", url.port or 80, timeout=600)
    try:
        conn.request(method, path, body, headers or {})
        response = conn.getresponse()
        data = response.read()
    finally:
        conn.close()
    if response.status >= 300:
        msg = f"{method} {path.partition('?')[0]}: HTTP {response.status}: {data.decode(errors='replace')[:300]}"
        raise RuntimeError(msg)
    return data


def query(url: str, database: str, sql: str) -> pa.Table:
    """Runs SQL on the server at ``url`` and returns the result, fetched as Parquet."""
    body = json.dumps({"db": database, "q": sql, "format": "parquet"}).encode()
    data = request(url, "POST", "/api/v3/query_sql", body, {"Content-Type": "application/json"})
    return pq.read_table(io.BytesIO(data)) if data else pa.table({})
