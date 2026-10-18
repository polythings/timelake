"""Timelake: ``Table.append`` per commit with inline maintenance; read by its own reader (the CLI) and plain DuckDB."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, override

from timebench import BENCH
from timebench.meta import version
from timebench.s3 import PROXY
from timebench.systems import Runner, System, arrow_result, duckdb_connection, duckdb_runner

if TYPE_CHECKING:
    import pyarrow as pa

    import timelake
    from timebench.results import IngestRecord
    from timebench.targets import Place
    from timebench.workloads import TableSpec

CLI = BENCH.parent / "target" / "release" / "timelake"
"""The CLI the ``timelake`` reader runs (``cargo build --release -p timelake-cli``)."""


class Timelake(System):
    """Timelake tables with inline auto-maintenance (size-tiered compaction and snapshot expiry on append)."""

    name = "timelake"
    title = "Timelake"
    readers = ("timelake", "duckdb")
    commands = frozenset({"timelake"})

    def __init__(self, place: Place, namespace: str) -> None:
        super().__init__(place, namespace)
        self.inline_compactions = 0
        self._lake: timelake.Lake | None = None
        self.tables: dict[str, timelake.Table] = {}

    @property
    def lake(self) -> timelake.Lake:
        """The lake, opened on first use (after ``reset``)."""
        import timelake

        if self._lake is None:
            options = PROXY.object_store_options() if self.target.s3 else None
            self._lake = timelake.Lake(self.place.root, storage_options=options)
        return self._lake

    @override
    def create(self, spec: TableSpec, schema: pa.Schema) -> None:
        name = f"{self.namespace}.{spec.name}"
        if spec.time_column:
            table = self.lake.create_table(
                name, schema, time_column=spec.time_column, series_key=list(spec.series_key), dedup=spec.dedup
            )
        else:
            table = self.lake.create_table(name, schema)
        self.tables[spec.name] = table

    @override
    def append(self, table: str, data: pa.Table) -> None:
        maintenance = self.tables[table].append(data).get("maintenance")
        if maintenance and maintenance["partitions_compacted"] > 0:
            self.inline_compactions += 1

    @override
    def maintain(self) -> None:
        for name in self.lake.tables():
            self.lake.table(name).maintain()

    @override
    def state(self) -> dict[str, Any]:
        tables = {n.split(".", 1)[1]: self.lake.table(n).location for n in self.lake.tables()}
        return {"root": self.lake.location, "namespace": self.namespace, "tables": tables}

    @override
    @classmethod
    def version(cls) -> dict[str, str]:
        return {"timelake": version("timelake") or "?", "duckdb": version("duckdb") or "?"}

    @override
    def ingest_extras(self) -> IngestRecord:
        return {"inline_compactions": self.inline_compactions}

    @override
    @classmethod
    def open_reader(cls, reader: str, place: Place, state: dict[str, Any]) -> Runner:
        if reader == "timelake":
            # Timelake's reader in this process, for probes and long-lived clients; single queries run the CLI.
            import timelake

            options = {**(PROXY.object_store_options() if place.target.s3 else {}), **reader_options()}
            lake = timelake.Lake(state["root"], storage_options=options)
            return lambda sql: arrow_result(lake.sql(sql, namespace=state["namespace"]))
        # Plain DuckDB over the Iceberg tables Timelake wrote.
        con = duckdb_connection(place.target)
        con.execute("INSTALL iceberg; LOAD iceberg")
        for t, location in state["tables"].items():
            con.execute(f"CREATE VIEW {t} AS SELECT * FROM iceberg_scan('{location}')")
        return duckdb_runner(con)

    @override
    @classmethod
    def reader_command(cls, reader: str, place: Place, state: dict[str, Any], runs: int) -> list[str]:
        store = PROXY.object_store_options() if place.target.s3 else {}
        options = [x for k, v in {**store, **reader_options()}.items() for x in ("-o", f"{k}={v}")]
        cmd = [str(CLI), "-w", state["root"], *options]
        cmd += ["sql", "--namespace", state["namespace"], "--bench-runs", str(runs)]
        cmd += [x for k, v in reader_settings().items() for x in ("--set", f"{k}={v}")]
        if place.target.s3:
            cmd += ["--proxy-admin", str(PROXY.admin)]
        return cmd


def reader_options() -> dict[str, str]:
    """Extra storage options for Timelake's readers, from ``TIMEBENCH_TIMELAKE_OPTIONS`` (``key=value,...``).

    For comparing settings such as ``timelake_cache=off`` on the same tables.
    """
    raw = os.environ.get("TIMEBENCH_TIMELAKE_OPTIONS", "")
    return dict(kv.split("=", 1) for kv in raw.split(",") if "=" in kv)


def reader_settings() -> dict[str, str]:
    """Session settings for the CLI reader, from ``TIMEBENCH_TIMELAKE_SETTINGS`` (``key=value,...``).

    For comparing settings such as ``datafusion.execution.parquet.pushdown_filters=true`` on the same tables.
    """
    raw = os.environ.get("TIMEBENCH_TIMELAKE_SETTINGS", "")
    return dict(kv.split("=", 1) for kv in raw.split(",") if "=" in kv)
