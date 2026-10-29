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
# From here on the archive holds each batch's rows together in arrival order, and a batch ended
# after BATCH_ROWS rows or when the output's BATCH_PERIOD_US timer fired.
FILE_ORDER_START_US = _micros(dt.datetime(2026, 5, 2, tzinfo=dt.UTC))

ACK_WAIT_US = 30 * SECOND_US  # NATS redelivered a message this long after its last delivery
EARLY_US, LATE_US = 1 * SECOND_US, 6 * SECOND_US  # a redelivery's processing before or after due
BATCH_ROWS = 2_000
BATCH_PERIOD_US = 900 * SECOND_US
PERIOD_S = 10  # a periodic subject is sent once per this many seconds of device time
PROBE_GAP_US = 2 * SECOND_US  # the widest gap between the counter rows bracketing a row
REPLAY_CHUNK = 100_000  # rows replayed between sweeps of expired redeliveries


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
    subject: npt.NDArray[np.int32]  # index into subjects
    content: npt.NDArray[np.uint64]  # 128-bit digest of the payload, two words per row
    uptime: npt.NDArray[np.int64]  # a heartbeat's uptime_sec, -1 for every other row
    subjects: tuple[str, ...]

    @classmethod
    def from_batches(
        cls, batches: Iterable[pa.RecordBatch], heartbeat: str | None = None
    ) -> Deliveries:
        """The archive's time, subject and payload columns, batch by batch."""
        times: list[npt.NDArray[np.int64]] = []
        subjects: list[npt.NDArray[np.int32]] = []
        uptimes: list[npt.NDArray[np.int64]] = []
        digests: list[npt.NDArray[np.uint64]] = []
        index: dict[str, int] = {}
        for batch in batches:
            stamped = pc.cast(batch.column("time"), pa.timestamp("us", tz="UTC"))
            times.append(stamped.cast(pa.int64()).to_numpy())
            names = batch.column("subject").to_pylist()
            payloads = batch.column("payload").to_pylist()
            subjects.append(np.fromiter((index.setdefault(s, len(index)) for s in names), np.int32))
            uptimes.append(
                np.fromiter(
                    (
                        json.loads(p)["uptime_sec"] if s == heartbeat else -1
                        for s, p in zip(names, payloads, strict=True)
                    ),
                    np.int64,
                )
            )
            digest = b"".join(
                hashlib.blake2b(p.encode(), digest_size=16).digest() for p in payloads
            )
            digests.append(np.frombuffer(digest, dtype="<u8").reshape(-1, 2))
        return cls(
            time_us=np.concatenate(times),
            subject=np.concatenate(subjects),
            content=np.concatenate(digests),
            uptime=np.concatenate(uptimes),
            subjects=tuple(index),
        )


def _subject_ids(d: Deliveries, names: Iterable[str]) -> list[int]:
    wanted = set(names)
    return [i for i, name in enumerate(d.subjects) if name in wanted]


def _rows_of(d: Deliveries, names: Iterable[str]) -> npt.NDArray[np.bool_]:
    """Whether each row belongs to one of the named subjects."""
    return np.isin(d.subject, _subject_ids(d, names))


def _earliest(d: Deliveries, *, per_day: bool) -> npt.NDArray[np.bool_]:
    """Whether each row is the earliest of its subject and payload, in the month or its day."""
    day = d.time_us // DAY_US if per_day else np.zeros(len(d.time_us), np.int64)
    order = np.lexsort(
        (np.arange(len(d.time_us)), d.time_us, d.content[:, 1], d.content[:, 0], d.subject, day)
    )
    key = np.stack([day[order], d.subject[order], d.content[order, 0], d.content[order, 1]])
    earliest = np.zeros(len(d.time_us), np.bool_)
    earliest[order[:1]] = True
    earliest[order[1:][np.any(key[:, 1:] != key[:, :-1], axis=0)]] = True
    return earliest


def _identical_to_earlier(d: Deliveries) -> npt.NDArray[np.bool_]:
    """Rows equal in time, subject and payload to an earlier row."""
    order = np.lexsort(
        (np.arange(len(d.time_us)), d.content[:, 1], d.content[:, 0], d.subject, d.time_us)
    )
    key = np.stack([d.time_us[order], d.subject[order], d.content[order, 0], d.content[order, 1]])
    again = np.zeros(len(d.time_us), np.bool_)
    again[order[1:][np.all(key[:, 1:] == key[:, :-1], axis=0)]] = True
    return again


def _block_open_until(d: Deliveries) -> npt.NDArray[np.int64]:
    """Per row, until when its batch was certainly still collecting, or -1 if unknown.

    While a batch collects, NATS redelivers each of its messages every 30 s, so a pause of more
    than ACK_WAIT_US + LATE_US between neighbouring rows ends a batch, and so does a restart,
    which stops deliveries for longer. A block between two such pauses holds whole batches.
    If its rows run forward and it spans less than BATCH_PERIOD_US less that pause, at most one
    of its batches was ended by the timer and all others hold BATCH_ROWS rows, so cutting every
    BATCH_ROWS rows from either end finds each batch's end. Every other block proves nothing.
    """
    n = len(d.time_us)
    open_until = np.full(n, -1, np.int64)
    pause = ACK_WAIT_US + LATE_US
    step = np.diff(d.time_us)
    bounds = [0, *(np.flatnonzero(step > pause) + 1).tolist(), n]
    backwards = np.flatnonzero(step < -SECOND_US) + 1
    for start, end in zip(bounds, bounds[1:], strict=False):
        block = d.time_us[start:end]
        runs_back = np.searchsorted(backwards, start, "right") < np.searchsorted(backwards, end)
        if (
            runs_back
            or block.min() < FILE_ORDER_START_US
            or block.max() - block.min() >= BATCH_PERIOD_US - pause
        ):
            continue
        cuts = sorted(
            {start, end, *range(start + BATCH_ROWS, end, BATCH_ROWS)}
            | set(range(end - BATCH_ROWS, start, -BATCH_ROWS))
        )
        for a, b in zip(cuts, cuts[1:], strict=False):
            open_until[a:b] = d.time_us[a:b].max()
    return open_until


def _probe_open_until(d: Deliveries, counters: npt.NDArray[np.bool_]) -> npt.NDArray[np.int64]:
    """Per row, until when its batch was certainly still collecting, shown by counter rows.

    A later row with a counter row's payload is that message redelivered, so no batch holding
    one of its deliveries was written before NATS sent it, at most LATE_US before it was
    processed. A row whose neighbouring counter rows in the file, before and after it, lie
    within PROBE_GAP_US around it shares its batch with one of them, so its batch collected at
    least until the earlier of the two last redeliveries.
    """
    n = len(d.time_us)
    open_until = np.full(n, -1, np.int64)
    rows = np.flatnonzero(counters & (d.time_us >= FILE_ORDER_START_US))
    if not len(rows):
        return open_until
    # When each counter row's message was last delivered: rows sorted by payload, then time.
    order = rows[
        np.lexsort((d.time_us[rows], d.content[rows, 1], d.content[rows, 0], d.subject[rows]))
    ]
    key = np.stack([d.subject[order], d.content[order, 0], d.content[order, 1]])
    new_payload = np.concatenate([[True], np.any(key[:, 1:] != key[:, :-1], axis=0)])
    last_of_payload = np.flatnonzero(np.concatenate([new_payload[1:], [True]]))
    last = np.zeros(n, np.int64)
    last[order] = d.time_us[order][last_of_payload][np.cumsum(new_payload) - 1]
    redelivered = np.where(last[rows] > d.time_us[rows] + EARLY_US, last[rows] - LATE_US, -1)
    # The counter rows before and after each row in the file.
    before = np.searchsorted(rows, np.arange(n), "right") - 1
    after = np.searchsorted(rows, np.arange(n), "left")
    has = (before >= 0) & (after < len(rows))
    i = np.flatnonzero(has)
    b, a = before[i], after[i]
    near = (d.time_us[rows[b]] >= d.time_us[i] - PROBE_GAP_US) & (
        d.time_us[rows[a]] <= d.time_us[i] + PROBE_GAP_US
    )
    near &= d.time_us[rows[b]] <= d.time_us[i]
    near &= d.time_us[rows[a]] >= d.time_us[i]
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
    rows = rows[np.lexsort((rows, d.time_us[rows]))]
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
    keep: npt.NDArray[np.bool_],
    repeats_30_s: npt.NDArray[np.bool_],
    among: npt.NDArray[np.bool_],
) -> tuple[npt.NDArray[np.bool_], npt.NDArray[np.bool_]]:
    """Periodic rows beyond what the device sent, and the rows counted.

    JetStream first-delivers in publishing order, so between two heartbeats' first deliveries
    arrive the messages published between them: per subject at most one per PERIOD_S of the
    uptime the heartbeats differ by, rounded up. While a backlog is caught up, rows are processed
    out of that order, so an interval's count stays usable until LATE_US after the next heartbeat
    that arrived live: no later against the device clock than the least delayed one since the
    device booted, give or take LATE_US. Rows of `keep` always stay; the others fill the count,
    those not repeating a delivery 30 s before first, then the earliest. Only rows of `among`
    count.
    """
    surplus = np.zeros(len(d.time_us), np.bool_)
    counted = np.zeros(len(d.time_us), np.bool_)
    if rules.heartbeat not in d.subjects:
        return surplus, counted
    beats = np.flatnonzero(_rows_of(d, [rules.heartbeat]) & _earliest(d, per_day=False))
    beats = beats[np.argsort(d.time_us[beats], kind="stable")]
    at, uptime = d.time_us[beats], d.uptime[beats]
    usable = (uptime[1:] > uptime[:-1]) & (at[:-1] >= WINDOW_START_US) & (at[1:] < WINDOW_END_US)
    allowed = (uptime[1:] - uptime[:-1] + PERIOD_S - 1) // PERIOD_S  # rounded up
    usable_until = _until_next_live(at, uptime)
    interval = np.searchsorted(at, d.time_us, side="right") - 1
    inside = (interval >= 0) & (interval < len(usable))
    inside[inside] = usable[interval[inside]]

    for subject in _subject_ids(d, rules.periodic):
        rows = np.flatnonzero((d.subject == subject) & inside & among)
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
    identical = _identical_to_earlier(d)
    # Each periodic row repeating a delivery 30 s before, proven or not, only to order them.
    repeats_30_s = _after_30_s(
        d, np.flatnonzero(window & periodic), np.full(len(d.time_us), np.iinfo(np.int64).max)
    )
    surplus, counted = _beyond_device_count(
        d, rules, _earliest(d, per_day=True), repeats_30_s, ~identical
    )
    collecting = np.maximum(_block_open_until(d), _probe_open_until(d, unique))
    proven = _after_30_s(d, np.flatnonzero(window & ~unique), collecting)
    reasons = np.select(
        [
            unique & ~_earliest(d, per_day=False),
            identical,
            surplus,
            proven & ~counted & ~unique,
        ],
        [Reason.SAME_PAYLOAD, Reason.IDENTICAL, Reason.BEYOND_COUNT, Reason.AFTER_30_S],
        Reason.KEPT,
    )
    return np.where(window, reasons, Reason.KEPT).astype(np.int8)


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
