"""Naive Iceberg (PyIceberg appends, the S3 Tables / Firehose write pattern) and real S3 Tables; read by DuckDB."""

from __future__ import annotations

import datetime as dt
import os
import time
from functools import cached_property
from typing import TYPE_CHECKING, Any, override

from timebench.meta import version
from timebench.s3 import PROXY
from timebench.systems import Runner, System, duckdb_connection, duckdb_runner

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table as IcebergTable

    from timebench.results import IngestRecord
    from timebench.targets import Place, StorageStats
    from timebench.workloads import TableSpec


class Iceberg(System):
    """PyIceberg appends into a SQLite-catalog warehouse, time-series tables partitioned by ``day(time)``.

    Maintenance rewrites each day partition of each time-series table, one at a time so a day is the most held in
    memory. Default: binpack compaction (S3 Tables' default: same rows, bigger files, no re-sort) of days with more
    than one file. Tuned: the tables declare a sort order on (series key, time), and compaction sorts every day by it,
    as Iceberg's docs recommend for filters on those columns.
    """

    name = "iceberg"
    title = "Iceberg (PyIceberg)"
    readers = ("duckdb",)
    tunings = ("tuned", "default")

    def __init__(self, place: Place, namespace: str) -> None:
        super().__init__(place, namespace)
        self.tables: dict[str, IcebergTable] = {}
        self.time_series: list[str] = []

    @cached_property
    def catalog(self) -> Catalog:
        """The catalog, opened on first use (after ``reset`` has cleared the local directory)."""
        from pyiceberg.catalog.sql import SqlCatalog

        root = self.place.root
        return SqlCatalog(
            "bench",
            uri=f"sqlite:///{self.place.local_dir}/catalog.db",
            warehouse=root if self.target.s3 else f"file://{root}",
            **(PROXY.pyiceberg_properties() if self.target.s3 else {}),
        )

    @override
    def create(self, spec: TableSpec, schema: pa.Schema) -> None:
        from pyiceberg.transforms import DayTransform, IdentityTransform

        self.catalog.create_namespace_if_not_exists(self.namespace)
        name = f"{self.namespace}.{spec.name}"
        table = self.catalog.create_table(name, schema=schema)
        if spec.time_column:
            with table.update_spec() as update:
                update.add_field(spec.time_column, DayTransform(), f"{spec.time_column}_day")
            if self.tuned:
                with table.update_sort_order() as order:
                    for column in [*spec.series_key, spec.time_column]:
                        order.asc(column, IdentityTransform())
            table = self.catalog.load_table(name)
            self.time_series.append(spec.name)
        self.tables[spec.name] = table

    @override
    def append(self, table: str, data: pa.Table) -> None:
        self.tables[table].append(data)

    @override
    def maintain(self) -> None:
        for name in self.time_series or self.loaded_time_series():
            table = self.catalog.load_table(f"{self.namespace}.{name}")
            time_column = table.schema().find_column_name(table.spec().fields[0].source_id)
            partitions = table.inspect.partitions()
            days = zip(partitions["partition"].to_pylist(), partitions["file_count"].to_pylist(), strict=True)
            # The table's sort order, as columns: empty for a default table, which binpacks instead.
            keys = [(table.schema().find_column_name(f.source_id), "ascending") for f in table.sort_order().fields]
            for day, files in days:
                if files < 2 and not keys:
                    continue
                start = dt.datetime.combine(next(iter(day.values())), dt.time(), dt.UTC)
                end = start + dt.timedelta(days=1)
                rows = f"{time_column} >= '{start.isoformat()}' AND {time_column} < '{end.isoformat()}'"
                data = table.scan(row_filter=rows).to_arrow()
                table.overwrite(data.sort_by(keys) if keys else data, overwrite_filter=rows)

    def loaded_time_series(self) -> list[str]:
        """The partitioned tables of an earlier process (``maintain`` without ``create``)."""
        names = [t for _, t in self.catalog.list_tables(self.namespace)]
        return [n for n in names if self.catalog.load_table(f"{self.namespace}.{n}").spec().fields]

    @override
    def state(self) -> dict[str, Any]:
        return {"namespace": self.namespace, "tables": self.current_tables()}

    def current_tables(self) -> dict[str, str]:
        """Each table's current metadata file, from the catalog."""
        names = [t for _, t in self.catalog.list_tables(self.namespace)]
        return {n: self.catalog.load_table(f"{self.namespace}.{n}").metadata_location for n in names}

    @override
    @classmethod
    def version(cls) -> dict[str, str]:
        return {"pyiceberg": version("pyiceberg") or "?", "duckdb": version("duckdb") or "?"}

    @override
    @classmethod
    def open_reader(cls, reader: str, place: Place, state: dict[str, Any]) -> Runner:
        # The tables' current versions, as a reader asking the catalog sees them (they change with every commit).
        tables = cls(place, state["namespace"]).current_tables() if "namespace" in state else state["tables"]
        con = duckdb_connection(place.target)
        con.execute("INSTALL iceberg; LOAD iceberg")
        for t, metadata in tables.items():
            con.execute(f"CREATE VIEW {t} AS SELECT * FROM iceberg_scan('{metadata}')")
        return duckdb_runner(con)


class S3Tables(Iceberg):
    """Real S3 Tables: the same appends committed through its Iceberg REST endpoint, and its own managed compaction.

    AWS targets only, with ``BENCH_TABLE_BUCKET_ARN`` set. Maintenance waits (up to ``BENCH_S3TABLES_WAIT_S``, 6 h by
    default) until S3 Tables has compacted every time-series table after ingest. Default: its compaction as set up
    (binpack). Tuned: the tables' sort order, and compaction set to sort by it.
    """

    name = "s3tables"
    title = "S3 Tables (PyIceberg)"
    s3_storage_prices = False  # S3 Tables bills storage, monitoring and compaction at its own prices (not modeled)
    poll_seconds = 120

    def __init__(self, place: Place, namespace: str) -> None:
        import boto3

        if not place.target.s3:
            msg = "S3 Tables runs on AWS targets only"
            raise SystemExit(msg)
        super().__init__(place, namespace)
        self.region = os.environ["AWS_REGION"]
        self.arn = os.environ["BENCH_TABLE_BUCKET_ARN"]
        self.wait_s = int(os.environ.get("BENCH_S3TABLES_WAIT_S", str(6 * 3600)))
        self.client = boto3.client("s3tables", region_name=self.region)
        self.compaction: dict[str, int] = {}

    @override
    @cached_property
    def catalog(self) -> Catalog:
        from pyiceberg.catalog.rest import RestCatalog

        return RestCatalog(
            "s3tables",
            uri=f"https://s3tables.{self.region}.amazonaws.com/iceberg",
            warehouse=self.arn,
            **{"rest.sigv4-enabled": "true", "rest.signing-name": "s3tables", "rest.signing-region": self.region},
            # Data and metadata files go through the counting proxy, which passes them to the table's bucket.
            **PROXY.pyiceberg_properties(),
        )

    @override
    def create(self, spec: TableSpec, schema: pa.Schema) -> None:
        super().create(spec, schema)
        if self.tuned and spec.time_column:
            self.client.put_table_maintenance_configuration(
                tableBucketARN=self.arn,
                namespace=self.namespace,
                name=spec.name,
                type="icebergCompaction",
                value={"status": "enabled", "settings": {"icebergCompaction": {"strategy": "sort"}}},
            )

    @override
    def reset(self) -> None:
        super().reset()
        if not self.catalog.namespace_exists(self.namespace):
            return
        for _, name in self.catalog.list_tables(self.namespace):
            self.client.delete_table(tableBucketARN=self.arn, namespace=self.namespace, name=name)

    @override
    def stats(self) -> StorageStats:
        """Table buckets can't be listed, so count from Iceberg metadata: live data files, plus manifests."""
        data_files = data_bytes = manifests = 0
        for _, name in self.catalog.list_tables(self.namespace):
            table = self.catalog.load_table(f"{self.namespace}.{name}")
            files = table.inspect.files()
            data_files += files.num_rows
            data_bytes += sum(files["file_size_in_bytes"].to_pylist())
            manifests += len(set(table.inspect.all_manifests()["path"].to_pylist()))  # listed once per snapshot
        return {
            "objects": data_files + manifests,
            "data_files": data_files,
            "data_bytes": data_bytes,
            "bytes": data_bytes,
        }

    @override
    def maintain(self) -> None:
        since = dt.datetime.fromisoformat(self.place.load_state()["ingested_at"])
        names = self.time_series or self.loaded_time_series()
        deadline = time.time() + self.wait_s
        while time.time() < deadline:
            for n in names:
                job = self.client.get_table_maintenance_job_status(
                    tableBucketARN=self.arn, namespace=self.namespace, name=n
                )
                compaction: dict[str, Any] = job["status"].get("icebergCompaction", {})
                ran = compaction.get("lastRunTimestamp")
                if ran and ran > since and compaction.get("status") == "Successful":
                    self.compaction[n] = round((ran - since).total_seconds())
            print(f"  waiting for S3 Tables compaction: {self.compaction or 'not yet'}", flush=True)
            if len(self.compaction) == len(names):
                return
            time.sleep(self.poll_seconds)
        print("  compaction did not run before the deadline", flush=True)

    @override
    def maintain_extras(self) -> IngestRecord:
        return {"compaction_after_ingest_s": self.compaction}
