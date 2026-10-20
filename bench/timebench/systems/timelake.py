"""Timelake: ``Table.append`` per commit with inline maintenance; read by its own reader (the CLI) and plain DuckDB."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, override

from timebench import BENCH
from timebench.s3 import S3Endpoint
from timebench.systems import (
    Runner,
    System,
    arrow_result,
    duckdb_iceberg_runner,
    package_versions,
    store_options,
)
from timebench.targets import Tuning

if TYPE_CHECKING:
    import pyarrow as pa

    import timelake
    from timebench.results import IngestRecord
    from timebench.targets import Place
    from timebench.workloads import TableSpec

CLI = BENCH.parent / "target" / "release" / "timelake"
"""The CLI the ``timelake`` reader runs (``cargo build --release -p timelake-cli``)."""


class Timelake(System):
    """Timelake tables with inline auto-maintenance (size-tiered compaction and snapshot expiry on append).

    Default: the contract only (time column, series key) and default settings. Tuned: also the workload's rollups
    (``tunings/<workload>/timelake.sql``), which inline maintenance keeps fresh and the reader routes queries to.
    """

    name = "timelake"
    title = "Timelake"
    readers = ("timelake", "duckdb")
    commands = frozenset({"timelake"})
    tunings = (Tuning.TUNED, Tuning.DEFAULT)

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
            options = store_options(self.place, S3Endpoint.object_store_options) or None
            self._lake = timelake.Lake(self.place.root, storage_options=options)
        return self._lake

    @override
    def create(self, spec: TableSpec, schema: pa.Schema) -> None:
        super().create(spec, schema)
        name = f"{self.namespace}.{spec.name}"
        if spec.time_column:
            table = self.lake.create_table(
                name, schema, time_column=spec.time_column, series_key=list(spec.series_key), dedup=spec.dedup
            )
        else:
            table = self.lake.create_table(name, schema)
        self.tables[spec.name] = table

    @override
    def run_statement(self, sql: str) -> None:
        self.lake.sql(sql, namespace=self.namespace)

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
    def version(cls, place: Place) -> dict[str, str]:
        return package_versions("timelake", "duckdb")

    @override
    def ingest_extras(self) -> IngestRecord:
        return {"inline_compactions": self.inline_compactions}

    @override
    @classmethod
    def open_reader(cls, reader: str, place: Place, state: dict[str, Any]) -> Runner:
        if reader == "timelake":
            # Timelake's reader in this process, for probes and long-lived clients; single queries run the CLI.
            import timelake

            config = place.config
            options = {**store_options(place, S3Endpoint.object_store_options), **config.timelake_options}
            lake = timelake.Lake(state["root"], storage_options=options, settings=dict(config.timelake_settings))
            return lambda sql: arrow_result(lake.sql(sql, namespace=state["namespace"]))
        return duckdb_iceberg_runner(place, state["tables"])  # plain DuckDB over the Iceberg tables Timelake wrote

    @override
    @classmethod
    def reader_command(cls, reader: str, place: Place, state: dict[str, Any], runs: int) -> list[str]:
        config = place.config
        store = {**store_options(place, S3Endpoint.object_store_options), **config.timelake_options}
        cmd = [str(CLI), "-w", state["root"], *(x for k, v in store.items() for x in ("-o", f"{k}={v}"))]
        cmd += ["sql", "--namespace", state["namespace"], "--bench-runs", str(runs)]
        cmd += [x for k, v in config.timelake_settings.items() for x in ("--set", f"{k}={v}")]
        if place.s3:
            cmd += ["--proxy-admin", str(place.s3.admin)]
        return cmd
