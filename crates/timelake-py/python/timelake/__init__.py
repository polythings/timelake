"""Timelake: the open time-series lakehouse.

Arrow data in, SQL and pyarrow Tables out, stored as plain Iceberg and Parquet on object storage or local disk:

>>> import datetime as dt
>>> import pyarrow as pa
>>> import timelake
>>> lake = timelake.Lake("warehouse")  # a local path, or a URL such as "s3://bucket/warehouse"
>>> schema = pa.schema([("time", pa.timestamp("us", tz="UTC")), ("device_id", pa.int32()), ("temp", pa.float64())])
>>> table = lake.create_table("iot.readings", schema, time_column="time", series_key=["device_id"])
>>> now = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
>>> batch = pa.table({"time": [now, now], "device_id": [1, 2], "temp": [20.5, 21.0]}, schema=schema)
>>> table.append(batch)["rows"]
2
>>> lake.sql("SELECT avg(temp) AS temp FROM iot.readings").to_pylist()
[{'temp': 20.75}]
>>> lake.latest("iot.readings").num_rows  # the newest row per device, from the last-value index
2
>>> table.maintain()["files_rewritten"]  # compaction, retention and snapshot expiry (also run inline)
0

Other engines read the same tables, e.g. DuckDB: ``SELECT * FROM iceberg_scan('<table.location>')``.
"""

from importlib.metadata import version

from timelake._timelake import ConflictError, Lake, Table
from timelake._types import (
    AppendResult,
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
    CacheStats,
    ColumnInfo,
    HasArrowSchema,
    MaintainReport,
    PartitionInfo,
    SnapshotInfo,
    TableInfo,
    UpgradeReport,
)

__version__ = version("timelake")

__all__ = [
    "AppendResult",
    "ArrowArrayExportable",
    "ArrowSchemaExportable",
    "ArrowStreamExportable",
    "CacheStats",
    "ColumnInfo",
    "ConflictError",
    "HasArrowSchema",
    "Lake",
    "MaintainReport",
    "PartitionInfo",
    "SnapshotInfo",
    "Table",
    "TableInfo",
    "UpgradeReport",
    "__version__",
]
