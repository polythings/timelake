"""Workloads: a dataset at a named scale, the order its rows arrive in, and the queries asked of it.

Adding a workload: subclass ``Workload`` in a module here and add it to ``workloads()``. Its queries live in
``queries/<suite>/queries.sql``, written once with the dialect placeholders of ``timebench.queries``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

from timebench import DATA, TUNING_SQL
from timebench.queries import apply_dialect, raw_queries
from timebench.results import INGESTED
from timebench.systems import DIALECTS
from timebench.targets import Tuning

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping
    from pathlib import Path

    import pyarrow as pa


@dataclass(frozen=True)
class TableSpec:
    """One table of a workload."""

    name: str
    time_column: str | None = None
    """None for a dimension table."""
    series_key: tuple[str, ...] = ()
    """The columns that identify one series (Timelake's series key; TimescaleDB's segment-by)."""
    dedup: bool = False
    """Rows may arrive twice: one row per series key and time counts, where the system can keep it so."""


@dataclass(frozen=True)
class Query:
    """One query, with the dialect placeholders still in."""

    suite: str
    name: str
    sql: str
    variants: Mapping[str, str] = field(default_factory=dict)
    """Reader -> its own version of the query, where its SQL lacks a construct the shared one uses; ``"<reader>
    tuned"`` -> the version a tuned run reads its precomputation with, as the system's docs write it (D23)."""

    def sql_for(self, reader: str, tuning: Tuning, now: int | None = None) -> str:
        """The query in a reader's SQL for a run of ``tuning``; ``now`` (epoch seconds) is the data's present."""
        tuned = self.variants.get(f"{reader} {Tuning.TUNED}") if tuning == Tuning.TUNED else None
        return apply_dialect(tuned or self.variants.get(reader, self.sql), DIALECTS[reader], now)


Batch = tuple[str, "pa.Table"]
"""Rows of one table, committed together."""


@dataclass(frozen=True)
class Stage:
    """A part of the stream, and the checkpoint the queries run at after it."""

    name: str
    arrivals: Iterable[list[Batch]]
    checkpoint: str | None
    """None: no queries after this stage."""
    live: bool = True
    """Arrivals are paced and probed (``--pace``, ``--live-interval``); a bulk stage loads as fast as it commits."""
    now: int | None = None
    """The data's present at the end of the stage (epoch seconds), for queries with ``@AGO``."""


class Workload(ABC):
    """A dataset and its queries."""

    name: ClassVar[str]
    title: ClassVar[str]
    scales: ClassVar[dict[str, str]]
    """Scale name -> what it means, smallest first."""
    default_scale: ClassVar[str]
    namespace: ClassVar[str]
    """The namespace (schema) the tables are created in."""
    tables: ClassVar[tuple[TableSpec, ...]]
    suites: ClassVar[tuple[str, ...]]
    """Query suites (``queries/<suite>``)."""
    dashboard_suite: ClassVar[str | None] = None
    """The suite a dashboard runs: probed during ingest and run by the concurrency clients."""
    dashboard: ClassVar[tuple[str, ...]] = ()
    """Only these queries of the dashboard suite (all of it when empty)."""
    live_table: ClassVar[str | None] = None
    """The time-series table whose newest timestamp tells how fresh a reader's view is."""
    span_days: float
    """How many days of data the workload holds (scales its costs to a month)."""

    def __init__(self, scale: str | None = None) -> None:
        scale = scale or self.default_scale
        if scale not in self.scales:
            msg = f"{self.name} has no scale {scale!r} (scales: {', '.join(self.scales)})"
            raise ValueError(msg)
        self.scale = scale

    @property
    def dataset(self) -> str:
        """The workload and scale, e.g. ``tsbs-iot-small``: names data and table directories."""
        return f"{self.name}-{self.scale}"

    @property
    def data_dir(self) -> Path:
        """Where the generated data lives."""
        return DATA / "workloads" / self.dataset

    def params(self) -> dict[str, object]:
        """Settings beyond the scale, recorded with the run."""
        return {}

    def spec(self, table: str) -> TableSpec:
        """A table's spec."""
        return next(t for t in self.tables if t.name == table)

    @abstractmethod
    def prepare(self) -> None:
        """Generates the data, unless it's already there."""

    @abstractmethod
    def schema(self, table: str) -> pa.Schema:
        """A table's schema."""

    @abstractmethod
    def initial(self) -> Iterator[Batch]:
        """Rows loaded before the stream starts (dimension tables), one commit each."""

    @abstractmethod
    def stream(self) -> Iterator[list[Batch]]:
        """The rows as they arrive: one list per arrival (a time window), one commit per table in it."""

    def stages(self) -> list[Stage]:
        """The stream in stages, each followed by a checkpoint or not; by default one live stage, then ``ingested``."""
        return [Stage("stream", self.stream(), INGESTED)]

    def checkpoints(self) -> list[str]:
        """The checkpoints of the stream's stages, in order (``maintained`` follows them)."""
        return [s.checkpoint for s in self.stages() if s.checkpoint]

    def queries(self) -> list[Query]:
        """All queries of the workload's suites."""
        out: list[Query] = []
        for suite in self.suites:
            plain: dict[str, str] = {}
            variants: dict[str, dict[str, str]] = {}
            for name, sql in raw_queries(suite):
                query, _, reader = name.partition(" ")
                if reader:  # `-- name: foo [clickhouse]`, or `[timelake tuned]` for tuned runs only
                    variants.setdefault(query, {})[reader.strip("[]")] = sql
                else:
                    plain[query] = sql
            out += [Query(suite, n, sql, variants.get(n, {})) for n, sql in plain.items()]
        return out

    def tuning_statements(self, system: str) -> list[str]:
        """What a tuned run of ``system`` runs after creating the tables: its precomputation (D23).

        The statements of ``tunings/<workload>/<system>.sql``, such as Timelake rollups; none without the file.
        """
        path = TUNING_SQL / self.name / f"{system}.sql"
        if not path.exists():
            return []
        lines = [line for line in path.read_text().splitlines() if not line.lstrip().startswith("--")]
        return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]

    def dashboard_queries(self) -> list[Query]:
        """The dashboard's queries (none if the workload has no dashboard suite)."""
        return [
            q
            for q in self.queries()
            if q.suite == self.dashboard_suite and (not self.dashboard or q.name in self.dashboard)
        ]

    def freshness_query(self) -> Query | None:
        """The newest timestamp of the live table, in whole epoch seconds."""
        if not self.live_table:
            return None
        time_column = self.spec(self.live_table).time_column
        # Placeholders take no nested parentheses: the maximum comes from a subquery.
        newest = f"SELECT max({time_column}) AS newest FROM {self.live_table}"
        sql = f"SELECT CAST(floor(@EPOCH(newest)) AS BIGINT) AS newest FROM ({newest}) AS t"
        return Query("probe", "freshness", sql)


def workloads() -> dict[str, type[Workload]]:
    """All workloads by name."""
    from timebench.workloads.iiot import Iiot
    from timebench.workloads.tsbs import TsbsDevops, TsbsIot

    return {w.name: w for w in (TsbsIot, TsbsDevops, Iiot)}
