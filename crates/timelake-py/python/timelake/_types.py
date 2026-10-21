"""Types of the values the native module takes and returns."""

from typing import Protocol, TypedDict


class ArrowSchemaExportable(Protocol):
    """An object that exports an Arrow schema through the Arrow PyCapsule interface, e.g. a ``pyarrow.Schema``."""

    def __arrow_c_schema__(self) -> object:
        """Returns a PyCapsule holding an ``ArrowSchema``."""
        ...


class HasArrowSchema(Protocol):
    """An object with an Arrow ``.schema``, e.g. a ``pyarrow.Table``."""

    @property
    def schema(self) -> ArrowSchemaExportable:
        """The object's Arrow schema."""
        ...


class ArrowArrayExportable(Protocol):
    """One batch of Arrow data, e.g. a ``pyarrow.RecordBatch``."""

    def __arrow_c_array__(self, requested_schema: object | None = None) -> tuple[object, object]:
        """Returns PyCapsules holding an ``ArrowSchema`` and an ``ArrowArray``."""
        ...


class ArrowStreamExportable(Protocol):
    """A stream of Arrow batches, e.g. a ``pyarrow.Table``, ``RecordBatchReader`` or ``polars.DataFrame``."""

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """Returns a PyCapsule holding an ``ArrowArrayStream``."""
        ...


class MaintainReport(TypedDict):
    """What a maintenance run did: the fields of the Rust ``MaintainReport``, as in the TypeScript client.

    ``more_to_compact`` says compaction left files a later run merges: a group of more files than one merge reads
    was merged in parts. ``seconds`` is the time the run took.
    """

    partitions_compacted: int
    files_rewritten: int
    files_written: int
    rows_rewritten: int
    bytes_rewritten: int
    duplicates_removed: int
    more_to_compact: bool
    partitions_dropped: int
    files_dropped: int
    rollups_refreshed: int
    rollup_rows: int
    snapshots_expired: int
    objects_deleted: int
    seconds: float


class CacheStats(TypedDict):
    """What a lake's block cache did since it was opened: blocks from memory or disk, and blocks fetched."""

    memory_hits: int
    disk_hits: int
    misses: int
    bytes_fetched: int


class UpgradeReport(TypedDict):
    """What ``Table.upgrade()`` did, or would do with ``dry_run``.

    ``steps`` is empty when the table was already current. ``Table.rollback(snapshot_before)`` undoes the upgrade
    while its snapshots are kept (until ``keep_snapshots_until_ms``, milliseconds since the epoch).
    """

    steps: list[str]
    version_before: int
    snapshot_before: int | None
    keep_snapshots_until_ms: int | None


class _AppendResult(TypedDict):
    rows: int
    files: int
    bytes: int
    version: int
    commit_ms: float
    attempts: int
    duplicate: bool


class AppendResult(_AppendResult, total=False):
    """Statistics of one append.

    ``maintenance`` is present when inline maintenance ran; ``maintenance_error`` when it failed after the append
    committed (the data is in the table; run ``Table.maintain()`` later instead of retrying the append). A skipped
    duplicate batch reports 0 rows, files and bytes with ``duplicate`` set.
    """

    maintenance: MaintainReport
    maintenance_error: str


class TableInfo(TypedDict):
    """Contract, size and layout of a table version."""

    name: str
    version: int
    location: str
    time_column: str | None
    series_key: list[str]
    chunk: str | None
    dedup: bool
    ingest_time_column: str | None
    rows: int
    files: int
    bytes: int
    snapshots: int
    manifests: int
    last_value_index: str | None
