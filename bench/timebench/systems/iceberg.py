"""Naive Iceberg (PyIceberg appends, the S3 Tables / Firehose write pattern) and real S3 Tables; read by DuckDB."""

from __future__ import annotations

import datetime as dt
import time
from functools import cached_property
from typing import TYPE_CHECKING, Any, Protocol, override

from timebench.config import AWS_REGION_ENV, TABLE_BUCKET_ENV, ConfigError
from timebench.s3 import S3Endpoint
from timebench.systems import Runner, System, duckdb_iceberg_runner, package_versions, store_options
from timebench.targets import Tuning

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
    tunings = (Tuning.TUNED, Tuning.DEFAULT)

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
            **store_options(self.place, S3Endpoint.pyiceberg_properties),
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
    def version(cls, place: Place) -> dict[str, str]:
        return package_versions("pyiceberg", "duckdb")

    @override
    @classmethod
    def open_reader(cls, reader: str, place: Place, state: dict[str, Any]) -> Runner:
        # The tables' current versions, as a reader asking the catalog sees them (they change with every commit).
        tables = cls(place, state["namespace"]).current_tables() if "namespace" in state else state["tables"]
        return duckdb_iceberg_runner(place, tables)


class TablesApi(Protocol):
    """The calls of boto3's ``s3tables`` client that ``S3Tables`` makes."""

    def put_table_maintenance_configuration(self, **kwargs: object) -> object:
        """Sets a table's maintenance."""
        ...

    def delete_table(self, **kwargs: object) -> object:
        """Deletes a table."""
        ...

    def get_table_maintenance_job_status(self, **kwargs: object) -> dict[str, Any]:
        """The state of a table's maintenance jobs."""
        ...


class S3Tables(Iceberg):
    """Real S3 Tables: the same appends committed through its Iceberg REST endpoint, and its own managed compaction.

    AWS targets only, with ``BENCH_TABLE_BUCKET_ARN`` set. Maintenance waits (up to ``BENCH_S3TABLES_WAIT_S``, 6 h by
    default) until S3 Tables has compacted every time-series table after ingest, and fails if it hasn't by then.
    Default: its compaction as set up (binpack). Tuned: the tables' sort order, and compaction set to sort by it.
    """

    name = "s3tables"
    title = "S3 Tables (PyIceberg)"
    aws_only = True
    s3_storage_prices = False  # S3 Tables bills storage, monitoring and compaction at its own prices (not modeled)
    poll_seconds = 120

    def __init__(self, place: Place, namespace: str) -> None:
        super().__init__(place, namespace)
        self.wait_s = place.config.s3tables_wait_s
        self.compaction: dict[str, int] = {}

    @property
    def region(self) -> str:
        """The table bucket's region."""
        if not self.place.config.aws_region:
            msg = f"S3 Tables needs {AWS_REGION_ENV}"
            raise ConfigError(msg)
        return self.place.config.aws_region

    @property
    def arn(self) -> str:
        """The table bucket."""
        if not self.place.config.table_bucket_arn:
            msg = f"S3 Tables needs {TABLE_BUCKET_ENV}"
            raise ConfigError(msg)
        return self.place.config.table_bucket_arn

    @cached_property
    def client(self) -> TablesApi:
        """The S3 Tables API."""
        import boto3

        return boto3.client("s3tables", region_name=self.region)

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
            **self.place.config.proxy.pyiceberg_properties(),
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
        if not self.target.s3:
            msg = "S3 Tables runs on AWS targets only"
            raise ConfigError(msg)
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
        missing = [n for n in names if n not in self.compaction]
        msg = f"S3 Tables compaction did not run within {self.wait_s} s on {', '.join(missing)}"
        raise TimeoutError(msg)

    @override
    def maintain_extras(self) -> IngestRecord:
        return {"compaction_after_ingest_s": self.compaction}
