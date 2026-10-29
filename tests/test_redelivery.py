from __future__ import annotations

import datetime as dt
import json

import pyarrow as pa

from nats_archive_compactor.redelivery import Deliveries, Reason, Rules, classify

# A moment inside the redelivery days, so every rule applies.
BASE = dt.datetime(2026, 5, 10, 10, tzinfo=dt.UTC)

Row = tuple[float, str, str]


def _deliveries(rows: list[Row], heartbeat: str | None = None) -> Deliveries:
    """Rows as (seconds after BASE, subject, payload), in the order the archive holds them."""
    batch = pa.record_batch(
        {
            "time": [(BASE + dt.timedelta(seconds=s)).isoformat() for s, _, _ in rows],
            "subject": [subject for _, subject, _ in rows],
            "payload": [payload for _, _, payload in rows],
        }
    )
    return Deliveries.from_batches([batch], heartbeat=heartbeat)


def _kept(rows: list[Row], rules: Rules, heartbeat: str | None = None) -> list[Row]:
    reasons = classify(_deliveries(rows, heartbeat), rules)
    return [row for row, reason in zip(rows, reasons, strict=True) if reason == Reason.KEPT]


RTC = "warp.rtc.time"


def test_a_repeat_of_a_message_with_a_counter_is_a_copy_at_any_gap() -> None:
    """Its payload names the second it was sent, so the same payload is the same message."""
    first, second = '{"second": 1}', '{"second": 2}'
    rows = [(0.0, RTC, first), (1.0, RTC, second), (3.0, RTC, first), (30.0, RTC, first)]

    assert _kept(rows, Rules(unique=frozenset({RTC}))) == rows[:2]


HEARTBEAT = "ems-esp.heartbeat"
MIXER = "ems-esp.mixer_data_hc1"
EMS = Rules(unique=frozenset({HEARTBEAT}), periodic=frozenset({MIXER}), heartbeat=HEARTBEAT)


def _heartbeat(seconds: float, uptime: int) -> Row:
    return (seconds, HEARTBEAT, json.dumps({"uptime_sec": uptime}))


def _mixer_rows_between(rows: list[Row], starts: list[float]) -> list[int]:
    """How many MIXER rows fall between consecutive heartbeat times."""
    return [
        sum(1 for s, subject, _ in rows if subject == MIXER and lo < s < hi)
        for lo, hi in zip(starts, starts[1:], strict=False)
    ]


def test_six_rows_per_heartbeat_minute_survive_a_redelivery_storm() -> None:
    """The device sends each periodic topic every 10 s, so a minute has six genuine rows."""
    unchanged = '{"temp": 21.5}'
    genuine = [(s + 5.0, MIXER, unchanged) for s in range(0, 120, 10)]
    storm = [(33.0 + i / 10, MIXER, unchanged) for i in range(9)] + [(97.5, MIXER, unchanged)]
    rows = sorted(
        [_heartbeat(0, 1000), _heartbeat(60, 1060), _heartbeat(120, 1120)] + genuine + storm
    )

    assert _mixer_rows_between(_kept(rows, EMS, HEARTBEAT), [0, 60, 120]) == [6, 6]


def test_rows_owed_by_a_burst_of_heartbeats_stay_after_it() -> None:
    """After an outage the backlog's heartbeats arrive at once, their minutes' rows later."""
    unchanged = '{"temp": 21.5}'
    beats = [_heartbeat(0, 1000), _heartbeat(240, 1060), _heartbeat(240.001, 1120)]
    beats += [_heartbeat(240.002, 1180), _heartbeat(300, 1300)]
    first_minute = [(s + 5.0, MIXER, unchanged) for s in range(0, 60, 10)]
    # Uptime 1060 to 1300 holds 24 genuine rows; six more are copies.
    backlog = [(250.0 + s, MIXER, unchanged) for s in range(30)]
    rows = sorted(beats + first_minute + backlog)

    kept = _kept(rows, EMS, HEARTBEAT)
    assert _mixer_rows_between(kept, [0, 240, 240.002, 300]) == [6, 0, 24]


def test_a_missing_heartbeat_allows_the_rows_of_its_device_time() -> None:
    unchanged = '{"temp": 21.5}'
    genuine = [(s + 5.0, MIXER, unchanged) for s in range(0, 120, 10)]
    copies = [(70.0, MIXER, unchanged), (71.0, MIXER, unchanged)]
    rows = sorted([_heartbeat(0, 1000), _heartbeat(120, 1120)] + genuine + copies)

    assert _mixer_rows_between(_kept(rows, EMS, HEARTBEAT), [0, 120]) == [12]


TEMPERATURE = "warp.esp32.temperature"
WARP = Rules(unique=frozenset({RTC}))


def _filler(start: float, end: float, step: float = 1.0) -> list[Row]:
    """Rows of a counter subject keeping a batch busy, each payload distinct."""
    count = round((end - start) / step)
    return [(start + i * step, RTC, json.dumps({"tick": start + i * step})) for i in range(count)]


def test_a_repeat_30_s_later_in_a_batch_still_open_is_a_copy() -> None:
    """The batch kept collecting past the redelivery, so NATS must have redelivered."""
    rows = sorted(_filler(0, 60) + [(0.5, TEMPERATURE, "33"), (30.5, TEMPERATURE, "33")])

    assert (30.5, TEMPERATURE, "33") not in _kept(rows, WARP)


def test_a_repeat_30_s_after_its_batch_was_written_stays() -> None:
    """A timer-ended batch of 21 rows, then a full one: the 2,000 rows counted from the end."""
    written = sorted(_filler(0, 20) + [(0.5, TEMPERATURE, "33")])
    full = sorted(_filler(20, 59.98, step=0.02) + [(30.5, TEMPERATURE, "33")])
    assert len(full) == 2000

    assert (30.5, TEMPERATURE, "33") in _kept(written + full, WARP)


def _counter_redelivered(start: int, end: int, times: int) -> list[Row]:
    """A counter row every second, each delivered `times` times 30 s apart."""
    return [
        (s + 30.0 * n, RTC, json.dumps({"second": s}))
        for s in range(start, end)
        for n in range(times)
    ]


def _jumbled(rows: list[Row]) -> list[Row]:
    """The rows in time order with one row from long before in the middle, as a retry leaves."""
    ordered = sorted(rows)
    return (
        ordered[: len(ordered) // 2] + [(-300.0, TEMPERATURE, "31")] + ordered[len(ordered) // 2 :]
    )


def test_counter_rows_redelivered_around_a_repeat_prove_its_batch_was_open() -> None:
    """Without clear batch ends in the file, the counter's own redeliveries show the batch."""
    rows = _jumbled(
        _counter_redelivered(0, 40, 3) + [(5.5, TEMPERATURE, "33"), (35.5, TEMPERATURE, "33")]
    )

    assert (35.5, TEMPERATURE, "33") not in _kept(rows, WARP)


def test_without_redelivered_counter_rows_a_repeat_in_an_unclear_batch_stays() -> None:
    rows = _jumbled(
        _counter_redelivered(0, 40, 1) + [(5.5, TEMPERATURE, "33"), (35.5, TEMPERATURE, "33")]
    )

    assert (35.5, TEMPERATURE, "33") in _kept(rows, WARP)


def test_a_repeat_across_a_place_a_full_batch_could_have_ended_stays() -> None:
    """2,100 rows in a row hold a 2,000-row batch either first or last; both are cut."""
    rows = sorted(_filler(0, 42, step=0.02) + [(1.0, TEMPERATURE, "33"), (31.0, TEMPERATURE, "33")])
    assert len(rows) == 2102

    assert (31.0, TEMPERATURE, "33") in _kept(rows, WARP)


def test_identical_rows_keep_one() -> None:
    """Two deliveries stamped in the same microsecond hold nothing the other does not."""
    twin = (0.5, TEMPERATURE, "33")

    assert _kept([twin, twin], WARP) == [twin]


def test_a_twin_counts_once_towards_the_device_count() -> None:
    """Six rows stay in the minute even when two of the deliveries share a microsecond."""
    unchanged = '{"temp": 21.5}'
    genuine = [(s + 5.0, MIXER, unchanged) for s in range(0, 60, 10)]
    rows = sorted([_heartbeat(0, 1000), _heartbeat(60, 1060), genuine[0]] + genuine)
    rows += [(40.0, MIXER, unchanged)]  # a copy the twin must not crowd out

    assert _mixer_rows_between(_kept(sorted(rows), EMS, HEARTBEAT), [0, 60]) == [6]


def test_rows_after_the_redelivery_days_are_untouched() -> None:
    """From 27 May 19:52 UTC NATS waited 20 min, so a repeat there is no 30-s redelivery."""
    after = (dt.datetime(2026, 5, 27, 20, tzinfo=dt.UTC) - BASE).total_seconds()
    rows = [(after, RTC, '{"second": 1}'), (after + 1, RTC, '{"second": 1}')]

    assert _kept(rows, WARP) == rows


def test_new_content_stays_even_beyond_the_device_count() -> None:
    """No content is lost; a seventh distinct value is a boundary slip, not a copy."""
    values = [(s + 5.0, MIXER, json.dumps({"temp": s})) for s in range(0, 70, 10)]
    rows = sorted([_heartbeat(0, 1000), _heartbeat(80, 1060)] + values)

    assert _mixer_rows_between(_kept(rows, EMS, HEARTBEAT), [0, 80]) == [7]


def test_a_genuine_return_to_an_earlier_value_outranks_copies() -> None:
    """Of the repeats, those not repeating a delivery from 30 s before fill the count first."""
    earlier, now = '{"temp": 21.0}', '{"temp": 21.5}'
    genuine = [(s + 5.0, MIXER, now) for s in range(0, 50, 10)] + [(55.0, MIXER, earlier)]
    copies = [(35.2, MIXER, now), (45.2, MIXER, now)]
    beats = [_heartbeat(0, 1000), _heartbeat(60, 1060)]
    rows = sorted([(-200.0, MIXER, earlier)] + beats + genuine + copies)

    assert (55.0, MIXER, earlier) in _kept(rows, EMS, HEARTBEAT)


def test_an_unused_count_expires_with_its_minute() -> None:
    """A minute the device sent nothing in leaves no room for copies in later minutes."""
    unchanged = '{"temp": 21.5}'
    beats = [_heartbeat(60.0 * m, 1000 + 60 * m) for m in range(4)]
    genuine = [(s + 5.0, MIXER, unchanged) for s in range(60, 180, 10)]
    copies = [(125.5 + s, MIXER, unchanged) for s in range(0, 54, 9)]
    rows = sorted(beats + genuine + copies)

    kept = _kept(rows, EMS, HEARTBEAT)
    assert _mixer_rows_between(kept, [0, 60, 120, 180]) == [0, 6, 6]


def test_a_falling_uptime_starts_the_count_afresh() -> None:
    """After a reboot the minute spanning it is not counted; the next one is."""
    unchanged = '{"temp": 21.5}'
    beats = [_heartbeat(0, 5000), _heartbeat(60, 5060), _heartbeat(70, 30), _heartbeat(130, 90)]
    rows = [(s, MIXER, unchanged) for s in (5.0, 15.0, 25.0, 35.0, 45.0, 55.0, 61.0, 62.0)]
    rows += [(s, MIXER, unchanged) for s in (75.0, 85.0, 95.0, 100.5, 105.0, 115.0, 125.0)]

    kept = _kept(sorted(beats + rows), EMS, HEARTBEAT)
    assert _mixer_rows_between(kept, [0, 60, 70, 130]) == [6, 2, 6]
