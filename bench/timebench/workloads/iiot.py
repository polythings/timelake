"""Industrial IoT (SPEC §17.6): a plant's process historian, from ``plant.py``'s generator, replayed as it arrived.

History loads in bulk, an hour of arrivals per commit, with queries after 1 and 7 days; the last day then arrives
live, a 5-minute window per commit, and the queries run again at ``ingested``. Queries ask about the last minutes,
hours or days before the data's present (``@AGO``), so they mean the same at every checkpoint.
"""

from __future__ import annotations

import json
import shutil
from typing import TYPE_CHECKING, ClassVar, override

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from timebench.workloads import Batch, Stage, TableSpec, Workload
from timebench.workloads.plant import (
    DAY,
    HOUR,
    READINGS,
    START,
    TAGS_PER_MACHINE,
    WINDOWS_PER_HOUR,
    generate,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

SEED = 20261015
GENERATOR_VERSION = 2
"""Bumped when the generator's output changes, so data generated before is made again."""
CHECKPOINT_DAYS = (1, 7)
"""History checkpoints, in days after the start."""


class Iiot(Workload):
    """A plant: sites, areas, lines and machines, 10 tags per machine, arriving through gateways that go offline."""

    name = "iiot"
    title = "Industrial IoT"
    machines_of: ClassVar[dict[str, int]] = {"small": 100, "standard": 1_000, "large": 10_000, "xlarge": 100_000}
    scales: ClassVar[dict[str, str]] = {
        "small": "1,000 tags (~10 MB/h raw), 30 days",
        "standard": "10,000 tags (~100 MB/h raw), 30 days",
        "large": "100,000 tags (~1 GB/h raw), 30 days",
        "xlarge": "1,000,000 tags (~10 GB/h raw), 7 days",
    }
    default_scale = "small"
    namespace = "iiot"
    tables = (TableSpec("tags"), TableSpec("readings", "time", ("tag",), dedup=True))
    suites = ("iiot",)
    dashboard_suite = "iiot"
    dashboard: ClassVar[tuple[str, ...]] = ("latest-area", "recent-machines", "trend-30d")
    live_table = "readings"

    def __init__(self, scale: str | None = None, *, days: int = 0, machines: int = 0) -> None:
        super().__init__(scale)
        self.days = days or (7 if self.scale == "xlarge" else 30)
        self.machines = machines or self.machines_of[self.scale]
        self.span_days = self.days

    @override
    def params(self) -> dict[str, object]:
        return {"days": self.days, "tags": self.machines * TAGS_PER_MACHINE, "seed": SEED}

    @property
    def marker(self) -> dict[str, object]:
        """What the data was generated with."""
        return {"version": GENERATOR_VERSION, "seed": SEED, "days": self.days, "machines": self.machines}

    @override
    def prepare(self) -> None:
        done = self.data_dir / "generated.json"
        if done.exists() and json.loads(done.read_text()) == self.marker:
            return
        tmp = self.data_dir.with_name(self.data_dir.name + ".tmp")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True)
        print(
            f"generating {self.dataset} ({self.machines} machines, {self.days} days) into {self.data_dir}", flush=True
        )
        counts = generate(tmp, self.machines, self.days, SEED)
        (tmp / "generated.json").write_text(json.dumps(self.marker))
        print(f"  {counts}", flush=True)
        shutil.rmtree(self.data_dir, ignore_errors=True)
        tmp.rename(self.data_dir)

    @override
    def schema(self, table: str) -> pa.Schema:
        return READINGS if table == "readings" else pq.read_schema(self.data_dir / "tags.parquet")

    @override
    def initial(self) -> Iterator[Batch]:
        yield "tags", pq.read_table(self.data_dir / "tags.parquet")

    def hour(self, h: int) -> pa.Table:
        """The rows that arrived in hour ``h``, in arrival order, with their window (0..11 within the hour)."""
        directory = self.data_dir / "readings" / f"h{h:05d}"
        if not directory.exists():
            return READINGS.empty_table().append_column("window", pa.array([], pa.int32()))
        parts = sorted(directory.glob("*.parquet"))
        table = pa.concat_tables(pq.read_table(p) for p in parts)
        # Filtering keeps the table's chunks, where `take` would concatenate them (and overflow a
        # string column past 2 GiB at a few hundred thousand tags).
        windows = table["window"].to_numpy()
        return pa.concat_tables(table.filter(pa.array(windows == w)) for w in np.unique(windows))

    def bulk(self, first: int, last: int) -> Iterator[list[Batch]]:
        """Arrival hours ``first``..``last``, one commit per hour."""
        for h in range(first, last):
            table = self.hour(h)
            if table.num_rows:
                yield [("readings", table.drop_columns(["window"]))]

    def live(self, first: int, last: int) -> Iterator[list[Batch]]:
        """Arrival hours ``first``..``last``, one commit per 5-minute window."""
        for h in range(first, last):
            table = self.hour(h)
            windows = table["window"].to_numpy()
            for w in range(h * WINDOWS_PER_HOUR, (h + 1) * WINDOWS_PER_HOUR):
                part = table.filter(pa.array(windows == w))
                yield [("readings", part.drop_columns(["window"]))] if part.num_rows else []

    @override
    def stream(self) -> Iterator[list[Batch]]:
        for stage in self.stages():
            yield from stage.arrivals

    @override
    def stages(self) -> list[Stage]:
        hours = self.days * DAY // HOUR
        live_from = hours - DAY // HOUR
        stages: list[Stage] = []
        done = 0
        for days in CHECKPOINT_DAYS:
            end = days * DAY // HOUR
            if end >= live_from:
                break
            stages.append(Stage(f"history-{days}d", self.bulk(done, end), f"{days}d", live=False, now=at(end)))
            done = end
        stages.append(Stage("history", self.bulk(done, live_from), None, live=False, now=at(live_from)))
        stages.append(Stage("live", self.live(live_from, hours), "ingested", now=at(hours)))
        return stages


def at(hour: int) -> int:
    """Epoch seconds ``hour`` hours after the start."""
    return START + hour * HOUR
