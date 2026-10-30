"""Telling NATS redeliveries from genuine messages in a month of the archive.

From 1 May 2026 until 27 May 2026 19:51:06 UTC, NATS redelivered every message 30 s after its
last delivery until a batch holding one of its deliveries was written to the archive, and every
delivery was stamped with the time it was processed. A stream's rules say per subject what
proves a row to be such a copy; a row nothing proves to be one stays.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from enum import IntEnum
from typing import Any

import numpy as np
import numpy.typing as npt
import pyarrow as pa
import pyarrow.compute as pc

SECOND_US = 1_000_000
DAY_US = 86_400 * SECOND_US


def _micros(moment: dt.datetime) -> int:
    return (moment - dt.datetime(1970, 1, 1, tzinfo=dt.UTC)) // dt.timedelta(microseconds=1)


WINDOW_START_US = _micros(dt.datetime(2026, 5, 1, tzinfo=dt.UTC))
# The last redelivery after 30 s arrived at 19:51:06 UTC.
WINDOW_END_US = _micros(dt.datetime(2026, 5, 27, 19, 52, tzinfo=dt.UTC))
ACK_WAIT_US = 30 * SECOND_US  # NATS redelivered a message this long after its last delivery
EARLY_US, LATE_US = 1 * SECOND_US, 6 * SECOND_US  # a redelivery's processing before or after due
# The archive output wrote a batch after this many rows or when its timer fired, every period.
BATCH_ROWS = 2_000
BATCH_PERIOD_US = 900 * SECOND_US
TIMER_SLACK_US = 2 * SECOND_US  # a batch ends this close before the timer fired
NEAR_TICKS = 16  # timer firings compared on either side; the timer drifts 4 ms per period
PERIOD_S = 10  # a periodic subject is sent once per this many seconds of device time
PROBE_GAP_US = 2 * SECOND_US  # the widest gap between the counter rows bracketing a row
REPLAY_CHUNK = 100_000  # rows replayed between sweeps of expired redeliveries
PROBE_CHUNK = 1_000_000  # rows whose neighbouring counter rows are looked up at a time


class Reason(IntEnum):
    """Why a row goes; a KEPT row stays."""

    KEPT = 0
    SAME_PAYLOAD = 1  # repeats a payload that carries a counter or clock
    IDENTICAL = 2  # equal in time, subject and payload to an earlier row
    BEYOND_COUNT = 3  # more rows than the device sent
    AFTER_30_S = 4  # redelivered 30 s after a delivery whose batch was still collecting


@dataclass(frozen=True)
class Rules:
    """What a stream's subjects guarantee about their genuine messages."""

    unique: frozenset[str] = frozenset()  # the payload carries a counter or clock
    periodic: frozenset[str] = frozenset()  # sent once per PERIOD_S of device time
    heartbeat: str | None = None  # its payload's uptime_sec is the device clock


@dataclass(frozen=True)
class Deliveries:
    """One month of a stream in file order, reduced to what telling copies apart needs."""

    time_us: npt.NDArray[np.int64]  # since the epoch
    subject: npt.NDArray[np.int16]  # index into subjects
    content: npt.NDArray[np.uint64]  # 128-bit digest of the payload, two words per row
    uptime: npt.NDArray[np.int32]  # a heartbeat's uptime_sec, -1 for every other row
    subjects: tuple[str, ...]

    @classmethod
    def from_batches(
        cls, batches: Iterable[pa.RecordBatch], rows: int, heartbeat: str | None = None
    ) -> Deliveries:
        """The archive's time, subject and payload columns of `rows` rows, batch by batch."""
        time_us = np.empty(rows, np.int64)
        subject = np.empty(rows, np.int16)
        content = np.empty((rows, 2), np.uint64)
        uptime = np.empty(rows, np.int32)
        index: dict[str, int] = {}
        at = 0
        for batch in batches:
            end = at + batch.num_rows
            stamped = pc.cast(batch.column("time"), pa.timestamp("us", tz="UTC"))
            time_us[at:end] = stamped.cast(pa.int64()).to_numpy()
            names = batch.column("subject").to_pylist()
            payloads = batch.column("payload").to_pylist()
            subject[at:end] = [index.setdefault(s, len(index)) for s in names]
            uptime[at:end] = [
                json.loads(p)["uptime_sec"] if s == heartbeat else -1
                for s, p in zip(names, payloads, strict=True)
            ]
            digest = b"".join(
                hashlib.blake2b(p.encode(), digest_size=16).digest() for p in payloads
            )
            content[at:end] = np.frombuffer(digest, dtype="<u8").reshape(-1, 2)
            at = end
        if at != rows:
            raise RuntimeError(f"read {at} rows, expected {rows}")
        return cls(time_us, subject, content, uptime, tuple(index))


def _subject_ids(d: Deliveries, names: Iterable[str]) -> list[int]:
    wanted = set(names)
    return [i for i, name in enumerate(d.subjects) if name in wanted]


def _rows_of(d: Deliveries, names: Iterable[str]) -> npt.NDArray[np.bool_]:
    """Whether each row belongs to one of the named subjects."""
    return np.isin(d.subject, _subject_ids(d, names))


def _same_as_previous(
    order: npt.NDArray[np.intp], columns: list[npt.NDArray[Any]]
) -> npt.NDArray[np.bool_]:
    """Whether each row in `order` but the first equals the row before it in every column."""
    same = np.ones(max(len(order) - 1, 0), np.bool_)
    for column in columns:
        values = column[order]
        same &= values[1:] == values[:-1]
    return same


@dataclass(frozen=True)
class _Payloads:
    """Per row, what the other deliveries of its subject and payload say about it."""

    first_in_month: npt.NDArray[np.bool_]  # the earliest of them
    first_in_day: npt.NDArray[np.bool_]  # the earliest of them on its UTC day
    identical: npt.NDArray[np.bool_]  # equal in time to an earlier one
    last_at: npt.NDArray[np.int64]  # when the last of them arrived


def _payloads(d: Deliveries) -> _Payloads:
    """Per row, its place among the rows of its subject and payload, ties in file order."""
    order = np.lexsort((d.time_us, d.content[:, 1], d.content[:, 0], d.subject))
    first = np.concatenate(
        [[True], ~_same_as_previous(order, [d.subject, d.content[:, 0], d.content[:, 1]])]
    )
    at = d.time_us[order]
    day = at // DAY_US
    first_in_day = first.copy()
    first_in_day[1:] |= day[1:] != day[:-1]
    del day
    identical = np.concatenate([[False], ~first[1:] & (at[1:] == at[:-1])])
    last_of_payload = np.flatnonzero(np.concatenate([first[1:], [True]]))
    last_at = at[last_of_payload][np.cumsum(first) - 1]
    del at, last_of_payload

    def by_row(sorted_values: npt.NDArray[Any]) -> npt.NDArray[Any]:
        values = np.empty_like(sorted_values)
        values[order] = sorted_values
        return values

    return _Payloads(by_row(first), by_row(first_in_day), by_row(identical), by_row(last_at))


def _block_open_until(d: Deliveries) -> npt.NDArray[np.int64]:
    """Per row, until when its batch was certainly still collecting, or -1 if unknown.

    The archive holds each batch's rows together in arrival order and the batches of an hour
    in random order, so time runs forward within a batch and jumps back between two. While a
    batch collects, NATS redelivers each of its messages every 30 s, so a pause of more than
    ACK_WAIT_US + LATE_US between neighbouring rows ends a batch as well. A run between such
    cuts holds whole batches that followed each other, each of BATCH_ROWS rows unless the timer
    ended it, so cutting every BATCH_ROWS rows from either end finds each batch's end, as long
    as the timer ended at most one of them. The timer fires every BATCH_PERIOD_US and a run
    shorter than a batch ends where it fired, so a run within which it fired twice proves
    nothing; nor does a piece spanning a period.
    """
    n = len(d.time_us)
    open_until = np.full(n, -1, np.int64)
    pause = ACK_WAIT_US + LATE_US
    step = np.diff(d.time_us)
    bounds = [0, *(np.flatnonzero((step > pause) | (step < -SECOND_US)) + 1).tolist(), n]
    runs = [
        (start, end, int(d.time_us[start:end].min()), int(d.time_us[start:end].max()))
        for start, end in zip(bounds, bounds[1:], strict=False)
        if d.time_us[start:end].min() >= WINDOW_START_US
    ]
    fired = np.array(sorted(last for start, end, _, last in runs if end - start < BATCH_ROWS))
    for start, end, first, last in runs:
        if last - first >= BATCH_PERIOD_US - pause and _timer_fired_twice(first, last, fired):
            continue
        cuts = sorted(
            {start, end, *range(start + BATCH_ROWS, end, BATCH_ROWS)}
            | set(range(end - BATCH_ROWS, start, -BATCH_ROWS))
        )
        for a, b in zip(cuts, cuts[1:], strict=False):
            piece = d.time_us[a:b]
            if piece.max() - piece.min() < BATCH_PERIOD_US:
                open_until[a:b] = piece.max()
    return open_until


def _timer_fired_twice(first: int, last: int, fired: npt.NDArray[np.int64]) -> bool:
    """Whether the timer fired at least twice after `first` by `last`, give or take the slack.

    `fired` holds moments it fired, in order; each one, shifted by whole periods, marks the
    firings around it.
    """
    reach = NEAR_TICKS * BATCH_PERIOD_US
    lo, hi = np.searchsorted(fired, [last - reach, last + reach])
    moments = fired[lo:hi, None] + BATCH_PERIOD_US * np.arange(-NEAR_TICKS, NEAR_TICKS + 1)
    within = (moments > first + TIMER_SLACK_US) & (moments <= last + TIMER_SLACK_US)
    return bool(np.any(within.sum(axis=1) >= 2))


def _probe_open_until(
    d: Deliveries, counters: npt.NDArray[np.bool_], last_at: npt.NDArray[np.int64]
) -> npt.NDArray[np.int64]:
    """Per row, until when its batch was certainly still collecting, shown by counter rows.

    A later row with a counter row's payload is that message redelivered, so no batch holding
    one of its deliveries was written before NATS sent it, at most LATE_US before it was
    processed. A row whose neighbouring counter rows in the file, before and after it, lie
    within PROBE_GAP_US around it shares its batch with one of them, so its batch collected at
    least until the earlier of the two last redeliveries.
    """
    n = len(d.time_us)
    open_until = np.full(n, -1, np.int64)
    rows = np.flatnonzero(counters & (d.time_us >= WINDOW_START_US))
    if not len(rows):
        return open_until
    last = last_at[rows]
    redelivered = np.where(last > d.time_us[rows] + EARLY_US, last - LATE_US, -1)
    del last
    # The counter rows before and after each row in the file, a stretch of rows at a time.
    for start in range(0, n, PROBE_CHUNK):
        i = np.arange(start, min(start + PROBE_CHUNK, n))
        b = np.searchsorted(rows, i, "right") - 1
        a = np.searchsorted(rows, i, "left")
        has = (b >= 0) & (a < len(rows))
        i, b, a = i[has], b[has], a[has]
        before_at, after_at, at = d.time_us[rows[b]], d.time_us[rows[a]], d.time_us[i]
        near = (before_at >= at - PROBE_GAP_US) & (before_at <= at)
        near &= (after_at <= at + PROBE_GAP_US) & (after_at >= at)
        open_until[i[near]] = np.minimum(redelivered[b[near]], redelivered[a[near]])
    return open_until


def _after_30_s(
    d: Deliveries, rows: npt.NDArray[np.int64], open_until: npt.NDArray[np.int64]
) -> npt.NDArray[np.bool_]:
    """Rows repeating a delivery from 30 s before whose batch collected until it fell due.

    NATS redelivers an unacked message ACK_WAIT_US after its last delivery, and a message is
    acked once a batch holding one of its deliveries is written, so such a redelivery exists.
    Each delivery accounts for one copy; any of the identical rows can be it.
    """
    copy = np.zeros(len(d.time_us), np.bool_)
    rows = rows[np.argsort(d.time_us[rows], kind="stable")]
    # Per subject and payload, the redeliveries still due: (due, batch collecting until).
    pending: dict[tuple[int, int, int], deque[tuple[int, int]]] = {}
    for start in range(0, len(rows), REPLAY_CHUNK):
        part = rows[start : start + REPLAY_CHUNK]
        for i, at, subject, c0, c1, until in zip(
            part.tolist(),
            d.time_us[part].tolist(),
            d.subject[part].tolist(),
            d.content[part, 0].tolist(),
            d.content[part, 1].tolist(),
            open_until[part].tolist(),
            strict=True,
        ):
            expected = pending.setdefault((subject, c0, c1), deque())
            while expected and expected[0][0] < at - LATE_US:
                expected.popleft()
            chain = next(
                (
                    k
                    for k, (due, collecting) in enumerate(expected)
                    if due <= at + EARLY_US and due <= collecting
                ),
                None,
            )
            if chain is not None:
                _, collecting = expected[chain]
                del expected[chain]
                copy[i] = True
                until = min(collecting, until)
            expected.append((at + ACK_WAIT_US, until))
        # Forget payloads whose last redelivery can no longer turn up.
        horizon = int(d.time_us[part[-1]]) - LATE_US
        pending = {key: due for key, due in pending.items() if due[-1][0] >= horizon}
    return copy


def _beyond_device_count(
    d: Deliveries,
    rules: Rules,
    payloads: _Payloads,
    repeats_30_s: npt.NDArray[np.bool_],
) -> tuple[npt.NDArray[np.bool_], npt.NDArray[np.bool_]]:
    """Periodic rows beyond what the device sent, and the rows counted.

    JetStream first-delivers in publishing order, so between two heartbeats' first deliveries
    arrive the messages published between them: per subject at most one per PERIOD_S of the
    uptime the heartbeats differ by, rounded up. While a backlog is caught up, rows are processed
    out of that order, so an interval's count stays usable until LATE_US after the next heartbeat
    that arrived live: no later against the device clock than the least delayed one since the
    device booted, give or take LATE_US. The earliest row of each payload per day always stays;
    the others fill the count, those not repeating a delivery 30 s before first, then the
    earliest. Identical rows don't count.
    """
    surplus = np.zeros(len(d.time_us), np.bool_)
    counted = np.zeros(len(d.time_us), np.bool_)
    if rules.heartbeat not in d.subjects:
        return surplus, counted
    beats = np.flatnonzero(_rows_of(d, [rules.heartbeat]) & payloads.first_in_month)
    beats = beats[np.argsort(d.time_us[beats], kind="stable")]
    at, uptime = d.time_us[beats], d.uptime[beats].astype(np.int64)
    usable = (uptime[1:] > uptime[:-1]) & (at[:-1] >= WINDOW_START_US) & (at[1:] < WINDOW_END_US)
    allowed = (uptime[1:] - uptime[:-1] + PERIOD_S - 1) // PERIOD_S  # rounded up
    usable_until = _until_next_live(at, uptime)
    interval = np.searchsorted(at, d.time_us, side="right") - 1
    inside = (interval >= 0) & (interval < len(usable))
    inside[inside] = usable[interval[inside]]
    keep = payloads.first_in_day

    for subject in _subject_ids(d, rules.periodic):
        rows = np.flatnonzero((d.subject == subject) & inside & ~payloads.identical)
        rows = rows[np.lexsort((d.time_us[rows], repeats_30_s[rows], ~keep[rows], interval[rows]))]
        counted[rows] = True
        starts = np.searchsorted(interval[rows], np.arange(len(usable) + 1))
        credits: deque[list[int]] = deque()  # [usable until, rows left], newest first
        for k in np.flatnonzero(usable).tolist():
            while credits and credits[-1][0] < at[k]:
                credits.pop()
            credits.appendleft([int(usable_until[k + 1]), int(allowed[k])])
            # A row takes its own interval's count first, an earlier one's only once that is used.
            for i in rows[starts[k] : starts[k + 1]].tolist():
                credit = next((c for c in credits if c[1] and c[0] >= d.time_us[i]), None)
                if credit is not None:
                    credit[1] -= 1
                elif not keep[i]:
                    surplus[i] = True
    return surplus, counted


def _until_next_live(
    at: npt.NDArray[np.int64], uptime: npt.NDArray[np.int64]
) -> npt.NDArray[np.int64]:
    """Per heartbeat, LATE_US after the first heartbeat from it on that arrived live."""
    delay = at - uptime * SECOND_US
    # A falling uptime is a reboot or a heartbeat delivered late; either starts a new run.
    run = np.cumsum(np.concatenate([[True], uptime[1:] <= uptime[:-1]]))
    least = np.full(run.max() + 1 if len(run) else 1, np.iinfo(np.int64).max)
    np.minimum.at(least, run, delay)
    live = delay <= least[run] + LATE_US
    until = np.full(len(at), np.iinfo(np.int64).max)
    upcoming = np.iinfo(np.int64).max
    for j in range(len(at) - 1, -1, -1):
        if live[j]:
            upcoming = int(at[j]) + LATE_US
        until[j] = upcoming
    return until


def classify(d: Deliveries, rules: Rules) -> npt.NDArray[np.int8]:
    """Why each row goes, or KEPT; a row matching several reasons counts under the first."""
    window = (d.time_us >= WINDOW_START_US) & (d.time_us < WINDOW_END_US)
    unique = _rows_of(d, rules.unique)
    periodic = _rows_of(d, rules.periodic)
    payloads = _payloads(d)
    identical = payloads.identical
    # A row written twice may be one delivery, so its twin vouches for no redelivery of its own.
    deliveries = window & ~identical
    # Each periodic row repeating a delivery 30 s before, proven or not, only to order them.
    repeats_30_s = _after_30_s(
        d, np.flatnonzero(deliveries & periodic), np.full(len(d.time_us), np.iinfo(np.int64).max)
    )
    surplus, counted = _beyond_device_count(d, rules, payloads, repeats_30_s)
    collecting = _block_open_until(d)
    np.maximum(collecting, _probe_open_until(d, unique, payloads.last_at), out=collecting)
    proven = _after_30_s(d, np.flatnonzero(deliveries & ~unique), collecting)
    reasons = np.select(
        [
            unique & ~payloads.first_in_month,
            identical,
            surplus,
            proven & ~counted & ~unique,
        ],
        [Reason.SAME_PAYLOAD, Reason.IDENTICAL, Reason.BEYOND_COUNT, Reason.AFTER_30_S],
        Reason.KEPT,
    )
    return np.where(window, reasons, Reason.KEPT).astype(np.int8)


def payloads_lost(d: Deliveries, keep: npt.NDArray[np.bool_]) -> int:
    """How many payloads of a subject on a UTC day keep no row that day or the day before.

    A copy is delivered within minutes of its original, so one past midnight may go.
    """
    day = d.time_us // DAY_US
    order = np.lexsort((day, d.content[:, 1], d.content[:, 0], d.subject))
    same = _same_as_previous(order, [d.subject, d.content[:, 0], d.content[:, 1]])
    day = day[order]
    starts = np.flatnonzero(np.concatenate([[True], ~(same & (day[1:] == day[:-1]))]))
    kept = np.logical_or.reduceat(keep[order], starts)
    day = day[starts]
    covered = kept.copy()
    covered[1:] |= same[starts[1:] - 1] & kept[:-1] & (day[1:] == day[:-1] + 1)
    return int(np.count_nonzero(~covered))


def tally(d: Deliveries, reasons: npt.NDArray[np.int8]) -> list[dict[str, str | int]]:
    """Per UTC day of the row time and subject: rows, rows kept and rows dropped per reason."""
    counts = (
        pa.table({"day": d.time_us // DAY_US, "subject": d.subject, "reason": reasons})
        .group_by(["day", "subject", "reason"])
        .aggregate([([], "count_all")])
        .to_pylist()
    )
    lines: dict[tuple[int, str], dict[str, str | int]] = {}
    for row in counts:
        day, subject = row["day"], d.subjects[row["subject"]]
        line = lines.setdefault(
            (day, subject),
            {"day": str(np.datetime64(day * 86_400, "s"))[:10], "subject": subject, "rows": 0}
            | {reason.name.lower(): 0 for reason in Reason},
        )
        line["rows"] = int(line["rows"]) + row["count_all"]
        line[Reason(row["reason"]).name.lower()] = row["count_all"]
    return [lines[key] for key in sorted(lines)]


_EMS_PERIODIC = frozenset(
    f"ems-esp.{topic}"
    for topic in (
        "boiler_data",
        "boiler_data_dhw",
        "mixer_data_hc1",
        "thermostat_data",
        "thermostat_data_dhw",
        "thermostat_data_hc1",
    )
)

# The streams stamped with their delivery time in May 2026. A subject named nowhere is
# checked for redeliveries after 30 s only.
RULES = {
    "ems_esp": Rules(
        unique=frozenset({"ems-esp.heartbeat"}),
        periodic=_EMS_PERIODIC,
        heartbeat="ems-esp.heartbeat",
    ),
    "warp_charge_manager": Rules(
        unique=frozenset({"warp.charge_manager.low_level_state", "warp.charge_manager.state"})
    ),
    "warp_charge_tracker": Rules(),
    "warp_evse": Rules(unique=frozenset({"warp.evse.low_level_state"})),
    "warp_meter": Rules(unique=frozenset({"warp.meters.0.errors"})),
    "warp_system": Rules(unique=frozenset({"warp.ntp.state", "warp.rtc.time"})),
}
