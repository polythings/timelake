"""Lance: ``write_dataset(mode="append")`` per commit; read by DuckDB and by DataFusion with Lance's scanner."""

from __future__ import annotations

import datetime as dt
import os
from typing import TYPE_CHECKING, Any, override

from timebench.meta import version
from timebench.s3 import PROXY, SETUP_PROXY
from timebench.systems import Runner, System, arrow_result, duckdb_connection, duckdb_runner

if TYPE_CHECKING:
    import pyarrow as pa

    from timebench.targets import Place
    from timebench.workloads import TableSpec


class Lance(System):
    """One Lance dataset per table, appended to per commit.

    Default: no maintenance. Tuned: maintenance is ``compact_files()`` and ``cleanup_old_versions()``, then scalar
    indexes as Lance's docs suggest for filters: a B-tree on each series-key column and a zone map on the time column.
    """

    name = "lance"
    title = "Lance"
    readers = ("duckdb", "datafusion")
    tunings = ("tuned", "default")

    def __init__(self, place: Place, namespace: str) -> None:
        super().__init__(place, namespace)
        self.options = PROXY.lance_options() if place.target.s3 else None
        self.created: set[str] = set()
        self.specs: dict[str, TableSpec] = {}

    def uri(self, table: str) -> str:
        """Where a table's dataset lives."""
        return f"{self.place.root}/{table}.lance"

    @override
    def create(self, spec: TableSpec, schema: pa.Schema) -> None:
        """Lance creates a dataset on its first write: this only notes the table's columns."""
        self.specs[spec.name] = spec

    @override
    def append(self, table: str, data: pa.Table) -> None:
        import lance

        mode = "append" if table in self.created else "create"
        lance.write_dataset(data, self.uri(table), mode=mode, storage_options=self.options)
        self.created.add(table)

    @override
    def maintain(self) -> None:
        import lance

        if not self.tuned:
            return
        options = self.options
        if options and os.environ.get("BENCH_LANCE_DIRECT"):
            # Lance's multipart uploads fail through tools/s3proxy on RustFS; compact directly against it.
            options = SETUP_PROXY.lance_options()
        state = self.place.load_state()
        for table in state["tables"]:
            lance.dataset(self.uri(table), storage_options=options).optimize.compact_files()
            ds = lance.dataset(self.uri(table), storage_options=options)
            ds.cleanup_old_versions(older_than=dt.timedelta(0))
            spec = state.get("specs", {}).get(table, {})
            for column in spec.get("series_key", []):
                ds.create_scalar_index(column, "BTREE")
            if spec.get("time_column"):
                ds.create_scalar_index(spec["time_column"], "ZONEMAP")

    @override
    def state(self) -> dict[str, Any]:
        saved = self.place.load_state() if not self.created else {}
        tables = self.created or set(saved["tables"])
        specs = {
            n: {"time_column": s.time_column, "series_key": list(s.series_key)} for n, s in self.specs.items()
        } or saved.get("specs", {})
        return {"tables": {t: self.uri(t) for t in sorted(tables)}, "specs": specs}

    @override
    @classmethod
    def version(cls) -> dict[str, str]:
        return {
            "pylance": version("pylance") or "?",
            "duckdb": version("duckdb") or "?",
            "datafusion": version("datafusion") or "?",
        }

    @override
    @classmethod
    def open_reader(cls, reader: str, place: Place, state: dict[str, Any]) -> Runner:
        import lance

        options = PROXY.lance_options() if place.target.s3 else None
        datasets = {t: lance.dataset(uri, storage_options=options) for t, uri in state["tables"].items()}
        if reader == "duckdb":
            con = duckdb_connection(place.target)
            for t, ds in datasets.items():
                con.register(t, ds)
            return duckdb_runner(con)

        from datafusion import SessionContext

        ctx = SessionContext()
        for t, ds in datasets.items():
            ctx.register_table(t, lance.FFILanceTableProvider(ds))
        return lambda sql: arrow_result(ctx.sql(sql).to_arrow_table())
