"""Type stub for the native module (Rust, built with PyO3)."""

from collections.abc import Mapping, Sequence
from typing import final

import pyarrow as pa

from timelake._types import (
    AppendResult,
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
    CacheStats,
    HasArrowSchema,
    MaintainReport,
    TableInfo,
    UpgradeReport,
)

__all__ = ["ConflictError", "Lake", "Table"]

class ConflictError(RuntimeError):
    """Another writer committed first and the operation could not be rebased onto its commit."""

@final
class Lake:
    """A warehouse of Timelake tables on any object store or local disk."""

    def __new__(
        cls,
        location: str,
        storage_options: Mapping[str, str] | None = None,
        settings: Mapping[str, str] | None = None,
    ) -> Lake:
        """Opens a warehouse.

        Args:
            location: A local path, or an ``s3://``, ``gs://``, ``az://`` or ``file://`` URL.
            storage_options: Object store options, such as ``{"aws_region": "us-east-1"}``.
            settings: Session settings for every query, such as ``{"read_dedup": "false"}``; they override the
                tables' own. ``datafusion.*`` keys set DataFusion options.

        Raises:
            ValueError: The location, an option or a setting is invalid.
        """

    @property
    def location(self) -> str:
        """The warehouse URL."""

    def create_table(
        self,
        name: str,
        schema: ArrowSchemaExportable | HasArrowSchema,
        time_column: str | None = None,
        series_key: Sequence[str] | None = None,
        chunk: str | None = None,
        retention: str | None = None,
        dedup: bool | None = None,
        ingest_time: bool | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> Table:
        """Creates a table.

        With ``time_column`` the table is a time-series table: partitioned by ``chunk``, sorted by
        ``(*series_key, time_column)``, with a last-value index and automatic maintenance.

        Args:
            name: ``"namespace.table"``, or ``"table"`` in the ``default`` namespace.
            schema: A pyarrow schema, or anything with a ``.schema`` such as a pyarrow Table.
            time_column: The timestamp column. Leave unset for a plain (dimension) table.
            series_key: Columns that identify a series, such as ``["device_id"]``.
            chunk: Time partition size: ``"hour"``, ``"day"`` or ``"month"``.
            retention: Drop partitions older than this duration, such as ``"90d"``.
            dedup: Keep only the latest write per ``(series_key, time)``.
            ingest_time: Add an ``_ingest_ts`` column with each row's insertion time (default: on
                for time-series tables). The latest insertion wins among rows with the same key.
            properties: Extra table properties.

        Raises:
            FileExistsError: The table already exists.
            ValueError: The name, schema, chunk, retention or a property value is invalid.
        """

    def table(self, name: str) -> Table:
        """Opens an existing table at its latest version.

        Raises:
            KeyError: The table doesn't exist.
        """

    def tables(self) -> list[str]:
        """Names of all tables as ``"namespace.table"``."""

    def drop_table(self, name: str) -> None:
        """Deletes a table and its files. Dropping a table that doesn't exist does nothing."""

    def sql(self, query: str, namespace: str = "default") -> pa.Table:
        """Runs SQL and returns the result. Unqualified table names resolve in ``namespace``.

        Besides queries, it runs ``CREATE``, ``DROP`` and ``REFRESH MATERIALIZED ROLLUP``, which return one row
        with the rollup and the rows it took.

        Raises:
            RuntimeError: The query failed to plan or run.
        """

    def cache_stats(self) -> CacheStats | None:
        """What the block cache did since the lake was opened, or ``None`` with the cache off.

        The cache is on by default for object stores (``timelake_cache`` in ``storage_options``).
        """

    def latest(self, table: str) -> pa.Table:
        """The newest row of every series of a table, from its last-value index.

        Raises:
            KeyError: The table doesn't exist.
        """

@final
class Table:
    """A Timelake table. Methods that commit (``append``, ``maintain``) advance this handle to the new version."""

    @property
    def name(self) -> str:
        """The full name, ``"namespace.table"``."""

    @property
    def location(self) -> str:
        """The table's root URL."""

    @property
    def version(self) -> int:
        """The version this handle is at."""

    @property
    def schema(self) -> pa.Schema:
        """The table's schema."""

    def append(
        self,
        data: ArrowArrayExportable | ArrowStreamExportable,
        batch_id: str | None = None,
        maintain: bool | None = None,
    ) -> AppendResult:
        """Appends Arrow data as one commit.

        Columns are matched by name and cast to the table's types; missing nullable columns become nulls.

        Args:
            data: A pyarrow RecordBatch, Table or RecordBatchReader, a polars DataFrame, or anything with
                ``__arrow_c_stream__``.
            batch_id: Idempotency key: a batch id that was already committed is skipped and reported with
                ``duplicate=True``.
            maintain: Overrides the table's auto-maintenance setting for this append.

        Raises:
            ValueError: The data doesn't fit the table schema.
            ConflictError: A concurrent commit could not be rebased.
        """

    def maintain(
        self, compact: bool = True, retention: bool = True, expire: bool = True, rollups: bool = True
    ) -> MaintainReport:
        """Compacts (sort and dedup), applies retention, refreshes the table's rollups and expires old snapshots.

        Raises:
            ConflictError: A concurrent commit could not be rebased.
        """

    def upgrade(self, dry_run: bool = False, compact: bool = True, keep_snapshots: str = "7d") -> UpgradeReport:
        """Brings the table to the current format: format version, last-value index, sorted and compacted files, head.

        Each step is a commit, and no snapshot expires for ``keep_snapshots`` (a duration such as ``"7d"``), so
        ``rollback(report["snapshot_before"])`` undoes it.

        Args:
            dry_run: Only report what would change.
            compact: Rewrite unsorted or small files, as ``maintain()`` does.
            keep_snapshots: How long every snapshot is kept after the upgrade.

        Raises:
            ConflictError: A concurrent commit could not be rebased.
        """

    def set_properties(self, properties: Mapping[str, str | None]) -> None:
        """Changes table settings, such as ``{"read_dedup": "false"}``; ``None`` returns one to its default.

        Settings are named with or without the ``timelake.`` prefix, with dashes or underscores. Keys outside it
        are Iceberg properties. The change is one commit of new metadata, with no new snapshot.

        Raises:
            ValueError: An unknown setting, a bad value, or one that is fixed when the table is created.
            ConflictError: A concurrent commit could not be rebased.
        """

    def rollback(self, snapshot_id: int) -> None:
        """Makes an earlier snapshot current again; later snapshots stay in the history until they expire.

        Raises:
            KeyError: The table has no snapshot with this id.
        """

    def refresh(self) -> None:
        """Reloads the latest committed version."""

    def info(self) -> TableInfo:
        """Contract, size and layout of the current version: the fields of ``timelake info --json``."""
