"""Lance: ``write_dataset(mode="append")`` per commit; read by DuckDB and by DataFusion with Lance's scanner."""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any, override

from timebench.s3 import S3Endpoint
from timebench.systems import (
    Runner,
    System,
    datafusion_runner,
    duckdb_connection,
    duckdb_runner,
    package_versions,
    store_options,
)
from timebench.targets import Tuning

if TYPE_CHECKING:
    import pyarrow as pa

    from timebench.targets import Place


class Lance(System):
    """One Lance dataset per table, appended to per commit.

    Default: no maintenance. Tuned: maintenance is ``compact_files()`` and ``cleanup_old_versions()``, then scalar
    indexes as Lance's docs suggest for filters: a B-tree on each series-key column and a zone map on the time column.
    """

    name = "lance"
    title = "Lance"
    readers = ("duckdb", "datafusion")
    tunings = (Tuning.TUNED, Tuning.DEFAULT)

    def __init__(self, place: Place, namespace: str) -> None:
        super().__init__(place, namespace)
        self.options = store_options(place, S3Endpoint.lance_options) or None
        self.created: set[str] = set()

    def uri(self, table: str) -> str:
        """Where a table's dataset lives."""
        return f"{self.place.root}/{table}.lance"

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
        state = self.place.load_state()
        specs = self.saved_specs()
        for table in state["tables"]:
            lance.dataset(self.uri(table), storage_options=self.options).optimize.compact_files()
            ds = lance.dataset(self.uri(table), storage_options=self.options)
            ds.cleanup_old_versions(older_than=dt.timedelta(0))
            spec = specs.get(table)
            for column in spec["series_key"] if spec else []:
                ds.create_scalar_index(column, "BTREE")
            if spec and spec["time_column"]:
                ds.create_scalar_index(spec["time_column"], "ZONEMAP")

    @override
    def state(self) -> dict[str, Any]:
        tables = self.created or set(self.place.load_state()["tables"])
        return {"tables": {t: self.uri(t) for t in sorted(tables)}, "specs": self.saved_specs()}

    @override
    @classmethod
    def version(cls, place: Place) -> dict[str, str]:
        return package_versions("pylance", "duckdb", "datafusion")

    @override
    @classmethod
    def open_reader(cls, reader: str, place: Place, state: dict[str, Any]) -> Runner:
        import lance

        options = store_options(place, S3Endpoint.lance_options) or None
        datasets = {t: lance.dataset(uri, storage_options=options) for t, uri in state["tables"].items()}
        if reader == "duckdb":
            con = duckdb_connection(place.s3)
            for t, ds in datasets.items():
                con.register(t, ds)
            return duckdb_runner(con)

        from datafusion import SessionContext

        ctx = SessionContext()
        for t, ds in datasets.items():
            ctx.register_table(t, lance.FFILanceTableProvider(ds))
        return datafusion_runner(ctx)
