"""A deterministic generator for the IIoT workload (spec 45): one plant's process historian, as it arrives.

The plant is a hierarchy: sites of 10 areas, areas of 10 lines, lines of 10 machines, and each machine has the same
10 tags (``SIGNALS``) at mixed rates: 1 s, 10 s and 60 s, and on change (with an hourly heartbeat). Values follow the
machine's state (running, stopped, fault), its pressure setpoint, slow drifts and a daily cycle, with noise.

What makes it a plant rather than a clean series:
- Pressure excursions above the alarm limit, with the machine's ``alarm`` tag set while they last.
- Energy counters that only grow, and sometimes reset (a meter replaced).
- Sensor gaps (rows that never arrive) and stuck sensors (a value that stops changing).
- Store-and-forward: each line's gateway (but every tenth, which is wired) goes offline for 1-12 hours now and then.
  Its rows are buffered and sent after it reconnects, an hour of backlog per 5-minute window, while live rows keep
  arriving. The last 5 minutes before an outage are sent again, and 1.5% of all rows arrive twice (at-least-once
  delivery).

Everything is drawn from one seed, so a scale always generates the same rows. Rows are written by arrival hour
(``readings/h<hour>/``) with their arrival window, so the workload can replay them in the order they arrived. Every
injected event is also written to ``events.parquet``, the ground truth for the agent benchmark (spec 46).
"""

from __future__ import annotations

import datetime as dt
import itertools
from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from pathlib import Path

START = int(dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc).timestamp())
DAY = 86_400
HOUR = 3_600
WINDOW = 300
"""Seconds per arrival window (a gateway's batch interval)."""
WINDOWS_PER_HOUR = HOUR // WINDOW
CHUNK = 100
"""Machines generated together (bounds memory at large scales)."""
GOOD, UNCERTAIN = 192, 64
"""OPC quality codes."""
DUPLICATE_SHARE = 0.015
RESEND_S = 300
"""How much of the data before an outage a gateway sends again after it reconnects."""
BACKLOG_PER_WINDOW_S = HOUR
"""How much backlog a reconnected gateway sends per arrival window."""

RUNNING, STOPPED, FAULT = 1, 0, 2


@dataclass(frozen=True)
class Signal:
    """One tag of every machine."""

    name: str
    kind: str
    """``analog``, ``counter`` or ``state`` (sent on change)."""
    rate_s: int
    """Seconds between samples; 0 = on change, with an hourly heartbeat."""
    unit: str
    running: float
    """Typical value while the machine runs (analog)."""
    stopped: float
    """Typical value while it doesn't."""
    noise: float
    drift: float
    """Amplitude of the slow drift."""
    lo_limit: float | None = None
    hi_limit: float | None = None


SIGNALS = (
    Signal("pressure", "analog", 1, "bar", 6.0, 1.0, 0.05, 0.15, 1.0, 8.0),
    Signal("flow", "analog", 10, "m3/h", 40.0, 0.0, 0.8, 3.0, None, 55.0),
    Signal("temperature", "analog", 10, "degC", 62.0, 24.0, 0.3, 2.0, None, 85.0),
    Signal("motor_current", "analog", 10, "A", 31.0, 0.0, 0.6, 2.5, None, 45.0),
    Signal("speed", "analog", 10, "rpm", 1450.0, 0.0, 4.0, 10.0, None, 1600.0),
    Signal("vibration", "analog", 60, "mm/s", 2.6, 0.3, 0.15, 0.5, None, 7.1),
    Signal("energy", "counter", 60, "kWh", 0.0, 0.0, 0.0, 0.0),
    Signal("state", "state", 0, "", 0.0, 0.0, 0.0, 0.0),
    Signal("alarm", "state", 0, "", 0.0, 0.0, 0.0, 0.0),
    Signal("setpoint", "state", 0, "bar", 0.0, 0.0, 0.0, 0.0),
)
TAGS_PER_MACHINE = len(SIGNALS)
SIGNAL = {s.name: i for i, s in enumerate(SIGNALS)}

READINGS = pa.schema(
    [
        ("time", pa.timestamp("us", tz="UTC")),
        ("tag", pa.string()),
        ("value", pa.float64()),
        ("quality", pa.int16()),
    ]
)
"""The table the systems load: one row per sample (the narrow layout historians use)."""
EVENTS = pa.schema(
    [
        ("kind", pa.string()),
        ("tag", pa.string()),
        ("machine", pa.string()),
        ("line", pa.string()),
        ("start", pa.timestamp("s", tz="UTC")),
        ("end", pa.timestamp("s", tz="UTC")),
        ("value", pa.float64()),
    ]
)


def machine_name(m: int) -> str:
    """``M00042``."""
    return f"M{m:05d}"


def line_name(m: int) -> str:
    """The line of machine ``m``: ``L0004``."""
    return f"L{m // 10:04d}"


def area_name(m: int) -> str:
    """The area of machine ``m``: ``A000``."""
    return f"A{m // 100:03d}"


def site_name(m: int) -> str:
    """The site of machine ``m``: ``S00``."""
    return f"S{m // 1000:02d}"


def tag_name(m: int, signal: str) -> str:
    """``M00042.pressure``."""
    return f"{machine_name(m)}.{signal}"


def tags_table(machines: int) -> pa.Table:
    """The asset model: every tag with its machine, line, area, site, signal, unit, rate and limits."""
    ms = [m for m in range(machines) for _ in SIGNALS]
    ss = [s for _ in range(machines) for s in SIGNALS]
    return pa.table(
        {
            "tag": [tag_name(m, s.name) for m, s in zip(ms, ss, strict=True)],
            "machine": [machine_name(m) for m in ms],
            "line": [line_name(m) for m in ms],
            "area": [area_name(m) for m in ms],
            "site": [site_name(m) for m in ms],
            "signal": [s.name for s in ss],
            "kind": [s.kind for s in ss],
            "unit": [s.unit for s in ss],
            "rate_s": pa.array([s.rate_s for s in ss], pa.int32()),
            "lo_limit": pa.array([s.lo_limit for s in ss], pa.float64()),
            "hi_limit": pa.array([s.hi_limit for s in ss], pa.float64()),
            "gateway": [f"G{m // 10:04d}" for m in ms],
        }
    )


@dataclass
class Intervals:
    """Time intervals of many owners (machines, tags or gateways), sorted by owner, then start."""

    owner: np.ndarray
    start: np.ndarray
    end: np.ndarray

    def covering(self, owner: np.ndarray, t: np.ndarray) -> np.ndarray:
        """For each ``(owner, t)``, the index of the interval holding ``t``, or -1."""
        if len(self.owner) == 0:
            return np.full(len(t), -1)
        key = self.owner.astype(np.int64) * (1 << 34) + self.start
        i = np.searchsorted(key, owner.astype(np.int64) * (1 << 34) + t, side="right") - 1
        valid = i >= 0
        i = np.where(valid, i, 0)
        hit = valid & (self.owner[i] == owner) & (t < self.end[i])
        return np.where(hit, i, -1)


def poisson_intervals(
    rng: np.random.Generator, owners: int, span: int, every_s: float, lengths: tuple[int, int]
) -> Intervals:
    """Non-overlapping intervals per owner: on average one per ``every_s``, each ``lengths`` (min, max) seconds."""
    owner, start, end = [], [], []
    for o in range(owners):
        t = START + int(rng.exponential(every_s))
        while t < START + span:
            length = int(rng.integers(lengths[0], lengths[1] + 1))
            owner.append(o)
            start.append(t)
            end.append(t + length)
            t += length + int(rng.exponential(every_s))
    return Intervals(np.array(owner, np.int64), np.array(start, np.int64), np.array(end, np.int64))


@dataclass
class States:
    """Each machine's state changes over the whole span, and its running seconds up to each change."""

    machine: np.ndarray
    time: np.ndarray
    state: np.ndarray
    running_before: np.ndarray

    def at(self, machine: np.ndarray, t: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """The state at ``t``, and the seconds the machine ran from the start up to ``t``."""
        key = self.machine * (1 << 34) + self.time
        i = np.searchsorted(key, machine.astype(np.int64) * (1 << 34) + t, side="right") - 1
        state = self.state[i]
        running = self.running_before[i] + np.where(state == RUNNING, t - self.time[i], 0)
        return state, running


def machine_states(rng: np.random.Generator, machines: int, span: int) -> States:
    """Runs of about 6 h, stops of about 1 h, and now and then a fault of about 20 minutes."""
    machine, time, state, running = [], [], [], []
    for m in range(machines):
        t, s, ran = START, int(rng.integers(0, 2)), 0
        while t < START + span:
            machine.append(m)
            time.append(t)
            state.append(s)
            running.append(ran)
            mean = {RUNNING: 6 * HOUR, STOPPED: HOUR, FAULT: 20 * 60}[s]
            length = max(60, int(rng.exponential(mean)))
            if s == RUNNING:
                ran += length
            t += length
            s = FAULT if s == RUNNING and rng.random() < 0.1 else (STOPPED if s == RUNNING else RUNNING)
    return States(
        np.array(machine, np.int64), np.array(time, np.int64), np.array(state, np.int8), np.array(running, np.int64)
    )


@dataclass
class Plan:
    """Everything drawn for the whole span before the samples: the plant's behavior over time."""

    machines: int
    span: int
    states: States
    setpoints: Intervals
    """Per machine: intervals with the pressure setpoint in ``setpoint_values``."""
    setpoint_values: np.ndarray
    excursions: Intervals
    """Per machine: pressure above the alarm limit; ``alarm`` is 1 while one lasts."""
    gaps: Intervals
    """Per tag (machine * 10 + signal): rows that never arrive."""
    stuck: Intervals
    """Per tag: the value stops changing."""
    outages: Intervals
    """Per gateway (line): offline, rows buffered."""
    energy_kw: np.ndarray
    """Per machine: power while running."""
    reset_at: np.ndarray
    """Per machine: when its energy meter is replaced (after the span: never)."""


def plan(machines: int, days: int, seed: int) -> Plan:
    """Draws the plant's behavior for ``days`` days."""
    span = days * DAY
    rng = np.random.default_rng([seed, 0])
    states = machine_states(rng, machines, span)
    setpoints = poisson_intervals(rng, machines, span, 2 * DAY, (DAY, 3 * DAY))
    # Setpoints change at interval starts; between intervals the default 6.0 bar applies.
    setpoint_values = rng.choice(np.array([5.5, 6.5, 7.0]), len(setpoints.owner))
    excursions = poisson_intervals(rng, machines, span, 3 * DAY, (2 * 60, 15 * 60))
    tags = machines * TAGS_PER_MACHINE
    gaps = poisson_intervals(rng, tags, span, 10 * DAY, (5 * 60, HOUR))
    stuck = poisson_intervals(rng, tags, span, 30 * DAY, (HOUR, 6 * HOUR))
    outages = poisson_intervals(rng, (machines + 9) // 10, span, 4 * DAY, (HOUR, 12 * HOUR))
    wireless = outages.owner % 10 != 0  # every tenth line's gateway is wired and stays online (the dashboards' line)
    outages = Intervals(outages.owner[wireless], outages.start[wireless], outages.end[wireless])
    energy_kw = rng.uniform(11, 55, machines)
    reset_at = np.where(rng.random(machines) < 0.3, START + rng.integers(0, span, machines), START + 2 * span)
    return Plan(
        machines, span, states, setpoints, setpoint_values, excursions, gaps, stuck, outages, energy_kw, reset_at
    )


@dataclass
class Points:
    """Sample points of one signal: each one's machine and time, and the machine's state and running seconds then."""

    machine: np.ndarray
    time: np.ndarray
    state: np.ndarray
    running: np.ndarray


def drift(rng: np.random.Generator, n: int, t: np.ndarray, owner: np.ndarray) -> np.ndarray:
    """A slow, smooth variation per owner (0..n) in [-1, 1]: three waves with periods of 20 minutes to 6 hours."""
    periods = rng.uniform(1200, 6 * HOUR, (n, 3))
    phases = rng.uniform(0, 2 * np.pi, (n, 3))
    x = (t - START).astype(np.float64)
    out = np.zeros(len(t))
    for j in range(3):
        out += np.sin(2 * np.pi * x / periods[owner, j] + phases[owner, j])
    return out / 3


def samples(plan_: Plan, seed: int, day: int, first: int, count: int) -> tuple[np.ndarray, ...]:
    """One day's rows of machines ``first``..``first + count``: ``(tag index, time, value, quality)``."""
    rng = np.random.default_rng([seed, 1, day, first])
    day_start = START + day * DAY
    out_tag, out_time, out_value = [], [], []
    machines = np.arange(first, first + count)
    for si, signal in enumerate(SIGNALS):
        tag = machines * TAGS_PER_MACHINE + si
        if signal.rate_s:
            phase = np.random.default_rng([seed, 3, si, first]).integers(0, signal.rate_s, count)
            n = DAY // signal.rate_s
            t = (day_start + phase[:, None] + signal.rate_s * np.arange(n)[None, :]).ravel()
            owner = np.repeat(tag, n)
        else:
            owner, t = state_changes(plan_, signal.name, machines, tag, day_start)
        m = owner // TAGS_PER_MACHINE
        points = Points(m, t, *plan_.states.at(m, t))
        if signal.kind == "analog":
            shape = np.random.default_rng([seed, 2, si, first])  # the tags' drifts: the same every day
            value = analog(plan_, signal, rng, drift(shape, count, t, m - first), points)
        elif signal.kind == "counter":
            value = energy(plan_, points)
        else:
            value = state_value(plan_, signal.name, points)
        out_tag.append(owner)
        out_time.append(t)
        out_value.append(value)
    tag = np.concatenate(out_tag)
    time = np.concatenate(out_time)
    value = np.concatenate(out_value)
    keep = plan_.gaps.covering(tag, time) < 0
    tag, time, value = tag[keep], time[keep], value[keep]
    value = freeze_stuck(plan_, tag, time, value)
    quality = np.where(rng.random(len(tag)) < 0.002, UNCERTAIN, GOOD).astype(np.int16)
    return tag, time, value, quality


def analog(plan_: Plan, signal: Signal, rng: np.random.Generator, wave: np.ndarray, p: Points) -> np.ndarray:
    """An analog signal: the state's level, plus drift, a daily cycle (temperature), excursions (pressure), noise."""
    m, t, state = p.machine, p.time, p.state
    running = state == RUNNING
    level = np.where(running, signal.running, signal.stopped)
    if signal.name == "pressure":
        level = np.where(running, setpoint(plan_, m, t), signal.stopped)
        bump = plan_.excursions.covering(m, t) >= 0
        level = np.where(bump & running, (signal.hi_limit or 0) + 1.5, level)
    if signal.name == "temperature":
        hour = ((t - START) % DAY) / HOUR
        level = level + 4.0 * np.sin(2 * np.pi * (hour - 9) / 24)
    if signal.name == "vibration":
        level = np.where(state == FAULT, 8.5, level)
    wave = wave * np.where(running, signal.drift, signal.drift / 4)
    return np.round(np.maximum(level + wave + rng.normal(0, signal.noise, len(t)), 0.0), 3)  # no negative readings


def setpoint(plan_: Plan, m: np.ndarray, t: np.ndarray) -> np.ndarray:
    """The pressure setpoint of each machine at ``t`` (6.0 bar outside the drawn intervals)."""
    i = plan_.setpoints.covering(m, t)
    return np.where(i >= 0, plan_.setpoint_values[np.maximum(i, 0)], 6.0)


def energy(plan_: Plan, p: Points) -> np.ndarray:
    """A cumulative kWh counter: grows while the machine runs; starts over from 0 when its meter is replaced."""
    m, t = p.machine, p.time
    kwh = plan_.energy_kw[m] * p.running / HOUR
    reset = plan_.reset_at[m]
    _, running_at_reset = plan_.states.at(m, np.minimum(reset, START + plan_.span))
    kwh_at_reset = plan_.energy_kw[m] * running_at_reset / HOUR
    return np.round(np.where(t >= reset, kwh - kwh_at_reset, kwh + 1000.0), 3)


def state_changes(
    plan_: Plan, signal: str, machines: np.ndarray, tag: np.ndarray, day_start: int
) -> tuple[np.ndarray, np.ndarray]:
    """The rows of an on-change tag in one day: its changes, and an hourly heartbeat."""
    day_end = day_start + DAY
    if signal == "state":
        changes = (plan_.states.machine, plan_.states.time)
    elif signal == "alarm":
        e = plan_.excursions
        changes = (np.concatenate([e.owner, e.owner]), np.concatenate([e.start, e.end]))
    else:
        s = plan_.setpoints
        changes = (np.concatenate([s.owner, s.owner]), np.concatenate([s.start, s.end]))
    owner_m, t = changes
    inside = (t >= day_start) & (t < day_end) & np.isin(owner_m, machines)
    owner_m, t = owner_m[inside], t[inside]
    beats = day_start + HOUR * np.arange(24)
    owner_m = np.concatenate([owner_m, np.repeat(machines, 24)])
    t = np.concatenate([t, np.tile(beats, len(machines))])
    tag_of = dict(zip(machines.tolist(), tag.tolist(), strict=True))
    owner = np.array([tag_of[x] for x in owner_m.tolist()], np.int64)
    order = np.lexsort((t, owner))
    owner, t = owner[order], t[order]
    first = np.ones(len(t), bool)
    first[1:] = (owner[1:] != owner[:-1]) | (t[1:] != t[:-1])  # a change on the hour is one row
    return owner[first], t[first]


def state_value(plan_: Plan, signal: str, p: Points) -> np.ndarray:
    """The value of an on-change tag: the machine state, the alarm (0/1), or the setpoint."""
    if signal == "state":
        return p.state.astype(np.float64)
    if signal == "alarm":
        return ((plan_.excursions.covering(p.machine, p.time) >= 0) & (p.state == RUNNING)).astype(np.float64)
    return setpoint(plan_, p.machine, p.time)


def freeze_stuck(plan_: Plan, tag: np.ndarray, time: np.ndarray, value: np.ndarray) -> np.ndarray:
    """Stuck sensors: inside a stuck interval, a tag repeats its first value in that interval (this day)."""
    i = plan_.stuck.covering(tag, time)
    hit = np.flatnonzero(i >= 0)
    if len(hit) == 0:
        return value
    hit = hit[np.lexsort((time[hit], i[hit]))]  # by interval, then time
    keys, firsts = np.unique(i[hit], return_index=True)
    value = value.copy()
    value[hit] = value[hit[firsts]][np.searchsorted(keys, i[hit])]
    return value


def arrivals(plan_: Plan, rng: np.random.Generator, tag: np.ndarray, time: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """When each row arrives (its window), and which rows arrive twice: ``(row index, window)`` of every copy."""
    window = (time - START) // WINDOW
    gateway = tag // TAGS_PER_MACHINE // 10
    out = plan_.outages
    rows = np.arange(len(time))
    resend_rows, resend_window = rows[:0], window[:0]
    if len(out.owner):
        i = out.covering(gateway, time)
        buffered = i >= 0
        reconnect = (out.end[np.maximum(i, 0)] - START) // WINDOW
        backlog = (time - out.start[np.maximum(i, 0)]) // BACKLOG_PER_WINDOW_S
        window = np.where(buffered, reconnect + backlog, window)
        # The last minutes before an outage are sent again when the gateway reconnects.
        j = out.covering(gateway, time + RESEND_S)
        resend = (j >= 0) & ~buffered & (time < out.start[np.maximum(j, 0)])
        resend_rows, resend_window = rows[resend], (out.end[j[resend]] - START) // WINDOW
    # At-least-once delivery: some rows come again in the next window.
    dup = rng.random(len(time)) < DUPLICATE_SHARE
    return np.concatenate([rows, resend_rows, rows[dup]]), np.concatenate([window, resend_window, window[dup] + 1])


def generate(out: Path, machines: int, days: int, seed: int) -> dict[str, int]:
    """Writes ``tags.parquet``, ``events.parquet`` and ``readings/h<hour>/*.parquet``; returns row counts."""
    plan_ = plan(machines, days, seed)
    tags = tags_table(machines)
    pq.write_table(tags, out / "tags.parquet", compression="zstd")
    names = tags["tag"].combine_chunks()
    last_window = days * DAY // WINDOW
    counts = {"rows": 0, "copies": 0, "late": 0}
    for day in range(days):
        for first in range(0, machines, CHUNK):
            count = min(CHUNK, machines - first)
            tag, time, value, quality = samples(plan_, seed, day, first, count)
            rows, window = arrivals(plan_, np.random.default_rng([seed, 4, day, first]), tag, time)
            keep = window < last_window  # rows still in a gateway's buffer at the end haven't arrived
            rows, window = rows[keep], window[keep]
            counts["rows"] += len(tag)
            counts["copies"] += len(rows)
            counts["late"] += int(np.sum(window != (time[rows] - START) // WINDOW))
            order = np.lexsort((time[rows], window))  # by arrival window, then time
            rows, window = rows[order], window[order]
            copies = pa.table(
                {
                    "time": pa.array(time[rows] * 1_000_000, pa.timestamp("us", tz="UTC")),
                    "tag": names.take(pa.array(tag[rows], pa.int32())),
                    "value": value[rows],
                    "quality": pa.array(quality[rows], pa.int16()),
                    "window": pa.array(window, pa.int32()),
                }
            )
            write_hours(out, f"d{day:03d}-m{first:06d}.parquet", copies)
    pq.write_table(events(plan_, names), out / "events.parquet", compression="zstd")
    return counts


def write_hours(out: Path, name: str, copies: pa.Table) -> None:
    """Writes rows, ordered by arrival window, into their arrival hour's directory as ``name``."""
    if copies.num_rows == 0:
        return
    hour = copies["window"].to_numpy() // WINDOWS_PER_HOUR
    cuts = [0, *(np.flatnonzero(np.diff(hour)) + 1).tolist(), len(hour)]
    for a, b in itertools.pairwise(cuts):
        directory = out / "readings" / f"h{int(hour[a]):05d}"
        directory.mkdir(parents=True, exist_ok=True)
        pq.write_table(copies.slice(a, b - a), directory / name, compression="zstd")


def events(plan_: Plan, names: pa.Array) -> pa.Table:
    """Every injected event: excursions, sensor gaps, stuck sensors, gateway outages, meter resets."""
    rows: list[dict[str, object]] = []

    def add(kind: str, intervals: Intervals, per: str) -> None:
        for o, s, e in zip(intervals.owner.tolist(), intervals.start.tolist(), intervals.end.tolist(), strict=True):
            m = o // TAGS_PER_MACHINE if per == "tag" else (o * 10 if per == "gateway" else o)
            tag = names[o].as_py() if per == "tag" else (tag_name(m, "pressure") if per == "machine" else None)
            rows.append(
                {
                    "kind": kind,
                    "tag": tag,
                    "machine": machine_name(m) if per != "gateway" else None,
                    "line": line_name(m),
                    "start": s,
                    "end": min(e, START + plan_.span),
                    "value": None,
                }
            )

    add("excursion", plan_.excursions, "machine")
    add("gap", plan_.gaps, "tag")
    add("stuck", plan_.stuck, "tag")
    add("outage", plan_.outages, "gateway")
    for m, at in enumerate(plan_.reset_at.tolist()):
        if at < START + plan_.span:
            rows.append(
                {
                    "kind": "meter-reset",
                    "tag": tag_name(m, "energy"),
                    "machine": machine_name(m),
                    "line": line_name(m),
                    "start": at,
                    "end": at,
                    "value": None,
                }
            )
    return pa.Table.from_pylist(rows, schema=EVENTS)
