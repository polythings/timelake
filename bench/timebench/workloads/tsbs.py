"""TSBS workloads (spec 44): TSBS's generator, its data converted once to Parquet, streamed in time windows.

IoT is a truck fleet's readings and diagnostics; DevOps (cpu-only) is servers' CPU metrics.
"""

from __future__ import annotations

import heapq
import itertools
import shutil
import subprocess
import sys
from typing import TYPE_CHECKING, ClassVar, override

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from timebench import BENCH
from timebench.workloads import Batch, TableSpec, Workload

if TYPE_CHECKING:
    from collections.abc import Iterator

SEED = 123
START, END = "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z"
"""Every scale covers the same 3 days, so the queries' fixed time windows (near the end) hold everywhere."""
GENERATOR = BENCH / ".bin" / "tsbs_generate_data"


Window = tuple[int, pa.Table]
"""A time window's number (since the epoch, in windows) and its rows."""


def windows(table: pa.Table, minutes: int) -> list[Window]:
    """Splits a table by time window, in time order; rows within a window keep their order."""
    us = pc.cast(table["time"], pa.int64()).to_numpy()
    w = us // (minutes * 60_000_000)
    order = np.argsort(w, kind="stable")
    table, w = table.take(order), w[order]
    cuts = np.flatnonzero(np.diff(w)) + 1
    bounds = [0, *cuts.tolist(), len(w)]
    return [(int(w[a]), table.slice(a, b - a)) for a, b in itertools.pairwise(bounds)]


def window_ids(path: str, minutes: int) -> np.ndarray:
    """The time window of every row of a table, in file order (reads only the time column)."""
    us = pc.cast(pq.read_table(path, columns=["time"])["time"], pa.int64()).to_numpy()
    return us // (minutes * 60_000_000)


def stream_windows(path: str, minutes: int) -> Iterator[Window]:
    """A table's time windows in order, as ``windows`` splits them.

    When the file is already in window order (TSBS DevOps), only one window is held in memory at a time; otherwise
    the whole table is (TSBS IoT has out-of-order rows).
    """
    ids = window_ids(path, minutes)
    if len(ids) and not np.all(ids[1:] >= ids[:-1]):
        yield from windows(pq.read_table(path), minutes)
        return
    pending: list[pa.Table] = []
    current, offset = None, 0
    for batch in pq.ParquetFile(path).iter_batches(batch_size=1_000_000):
        part = pa.Table.from_batches([batch])
        batch_ids = ids[offset : offset + part.num_rows]
        offset += part.num_rows
        cuts = [0, *(np.flatnonzero(np.diff(batch_ids)) + 1).tolist(), part.num_rows]
        for a, b in itertools.pairwise(cuts):
            window = int(batch_ids[a])
            if current is not None and window != current:
                yield current, pa.concat_tables(pending)
                pending = []
            current = window
            pending.append(part.slice(a, b - a))
    if current is not None:
        yield current, pa.concat_tables(pending)


def by_window(tables: dict[str, Iterator[Window]]) -> Iterator[list[Batch]]:
    """One arrival per time window: the rows of every table that has some in it, in window order."""

    def named(name: str, windows_: Iterator[Window]) -> Iterator[tuple[int, Batch]]:
        return ((w, (name, data)) for w, data in windows_)

    merged = heapq.merge(*(named(n, ws) for n, ws in tables.items()), key=lambda x: x[0])  # stable: table order
    for _, arrival in itertools.groupby(merged, key=lambda x: x[0]):
        yield [batch for _, batch in arrival]


class TsbsWorkload(Workload):
    """A TSBS use case, from TSBS's own generator and seed, appended in time windows.

    Windows are 5 minutes by default (Firehose's default buffer interval); larger windows load in bulk.
    """

    use_case: ClassVar[str]
    """TSBS's ``--use-case``."""
    counts: ClassVar[dict[str, int]]
    """TSBS's ``--scale`` (trucks, hosts) of each scale."""
    tag_column: ClassVar[str | None] = None
    """A tag copied into the measurement tables (TSBS's in-table partition tag)."""
    span_days = 3

    def __init__(self, scale: str | None = None, *, window_minutes: int = 5, max_windows: int = 0) -> None:
        super().__init__(scale)
        self.window_minutes = window_minutes
        self.max_windows = max_windows

    @override
    def params(self) -> dict[str, object]:
        params: dict[str, object] = {"window_minutes": self.window_minutes, "seed": SEED}
        if self.max_windows:
            params["max_windows"] = self.max_windows
        return params

    def path(self, table: str) -> str:
        """A table's Parquet file."""
        return str(self.data_dir / f"{table}.parquet")

    @override
    def prepare(self) -> None:
        if all((self.data_dir / f"{t.name}.parquet").exists() for t in self.tables):
            return
        if not GENERATOR.exists():
            msg = f"{GENERATOR} is missing: see bench/README.md, Setup"
            raise SystemExit(msg)
        tmp = self.data_dir.with_name(self.data_dir.name + ".tmp")
        shutil.rmtree(tmp, ignore_errors=True)
        generate = [
            str(GENERATOR),
            f"--use-case={self.use_case}",
            f"--seed={SEED}",
            f"--scale={self.counts[self.scale]}",
            f"--timestamp-start={START}",
            f"--timestamp-end={END}",
            "--log-interval=10s",
            "--format=timescaledb",
        ]
        convert = [sys.executable, str(BENCH / "scripts" / "tsbs_to_parquet.py"), str(tmp)]
        convert += ["--tag-column", self.tag_column] if self.tag_column else []
        print(f"generating {self.dataset} into {self.data_dir}", flush=True)
        with subprocess.Popen(generate, stdout=subprocess.PIPE) as gen:
            subprocess.run(convert, stdin=gen.stdout, check=True)
        if gen.returncode:
            msg = f"tsbs_generate_data failed with exit code {gen.returncode}"
            raise RuntimeError(msg)
        tmp.rename(self.data_dir)

    @override
    def schema(self, table: str) -> pa.Schema:
        return pq.read_schema(self.path(table))

    @override
    def initial(self) -> Iterator[Batch]:
        yield "tags", pq.read_table(self.path("tags"))

    @override
    def stream(self) -> Iterator[list[Batch]]:
        names = [t.name for t in self.tables if t.time_column]
        arrivals = by_window({n: stream_windows(self.path(n), self.window_minutes) for n in names})
        yield from itertools.islice(arrivals, self.max_windows or None)


class TsbsIot(TsbsWorkload):
    """TSBS IoT: a truck fleet's readings (location, speed, fuel use) and diagnostics (fuel, load, status)."""

    name = "tsbs-iot"
    title = "TSBS IoT"
    use_case = "iot"
    counts: ClassVar[dict[str, int]] = {"small": 50, "standard": 500, "large": 5000}
    scales: ClassVar[dict[str, str]] = {
        "small": "50 trucks, 3 days at 10 s (1.2M readings + 1.2M diagnostics)",
        "standard": "500 trucks, 3 days at 10 s (11.7M readings + 11.7M diagnostics)",
        "large": "5,000 trucks, 3 days at 10 s (117M readings + 117M diagnostics)",
    }
    default_scale = "standard"
    namespace = "tsbs"
    tables = (
        TableSpec("tags"),
        TableSpec("readings", "time", ("tags_id",)),
        TableSpec("diagnostics", "time", ("tags_id",)),
    )
    suites = ("tsbs_iot", "iot_point")
    dashboard_suite = "iot_point"
    live_table = "readings"


class TsbsDevops(TsbsWorkload):
    """TSBS DevOps (cpu-only): 10 CPU metrics per host every 10 seconds; all 15 DevOps query types read it."""

    name = "tsbs-devops"
    title = "TSBS DevOps"
    use_case = "cpu-only"
    counts: ClassVar[dict[str, int]] = {"small": 100, "standard": 4000}
    scales: ClassVar[dict[str, str]] = {
        "small": "100 hosts, 3 days at 10 s (2.6M rows)",
        "standard": "4,000 hosts, 3 days at 10 s (104M rows): the size TSBS results are published at",
    }
    default_scale = "standard"
    namespace = "devops"
    tag_column = "hostname"
    tables = (TableSpec("tags"), TableSpec("cpu", "time", ("hostname",)))
    suites = ("tsbs_devops",)
    dashboard_suite = "tsbs_devops"
    dashboard: ClassVar[tuple[str, ...]] = (
        "single-groupby-1-1-1",
        "single-groupby-1-1-12",
        "single-groupby-5-1-1",
        "cpu-max-all-1",
        "high-cpu-1",
        "groupby-orderby-limit",
    )
    live_table = "cpu"
