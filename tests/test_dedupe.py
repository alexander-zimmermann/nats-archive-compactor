from __future__ import annotations

import datetime as dt
from collections.abc import Iterator

import pyarrow as pa
import pytest
from botocore.exceptions import ClientError

import nats_archive_compactor.__main__ as compactor
from nats_archive_compactor.__main__ import _dedupe, _rollup

from .support import Store

Row = tuple[str, str, str]

A: Row = ("2026-05-02T10:00:00.1Z", "knx.1/2/3", '{"value": 1}')
B: Row = ("2026-05-02T10:00:00.2Z", "knx.1/2/3", '{"value": 2}')
C: Row = ("2026-05-03T09:00:00.3Z", "knx.1/2/4", '{"value": 3}')


def _archive_table(rows: list[Row]) -> pa.Table:
    """Rows given as (time, subject, payload), stored in the archive's column order."""
    return pa.table(
        {
            "payload": [row[2] for row in rows],
            "subject": [row[1] for row in rows],
            "time": [row[0] for row in rows],
        }
    )


def _rolled_up_month(store: Store, stream: str, days: dict[str, list[Row]]) -> pa.Table:
    """A May monthly file made by the rollup from these days; returns its rows as written."""
    for day, rows in days.items():
        store.put_table(f"{stream}/2026/05/{day}/daily.parquet", _archive_table(rows))
    _rollup(store.client, stream, "2026/05")
    return store.table(f"{stream}/2026/05/monthly.parquet")


def _rows(table: pa.Table) -> list[Row]:
    """The table's rows as (time, subject, payload)."""
    columns = (table.column(name).to_pylist() for name in ("time", "subject", "payload"))
    return list(zip(*columns, strict=True))


def test_a_deduped_month_holds_each_row_once_in_its_first_place(store: Store) -> None:
    """Exact repeats go; the original stays beside the monthly file."""
    original = _rolled_up_month(store, "knx", {"02": [A, B, A, A, B], "03": [C]})

    assert _dedupe(store.client, "knx", "2026/05") == "deduped"

    assert store.keys() == ["knx/2026/05/monthly.parquet", "knx/2026/05/monthly.parquet.orig"]
    assert _rows(store.table("knx/2026/05/monthly.parquet")) == [A, B, C]
    assert store.table("knx/2026/05/monthly.parquet.orig").equals(original)


def test_a_second_run_rewrites_from_the_original(store: Store) -> None:
    """A run cut off after the backup is finished by the next, which reads the backup."""
    original = _rolled_up_month(store, "knx", {"02": [A, B, A]})
    _dedupe(store.client, "knx", "2026/05")

    assert _dedupe(store.client, "knx", "2026/05") == "deduped"

    assert _rows(store.table("knx/2026/05/monthly.parquet")) == [A, B]
    assert store.table("knx/2026/05/monthly.parquet.orig").equals(original)


def test_a_rewrite_missing_a_row_never_replaces_the_monthly_file(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every row left out must have an identical row kept; the rewrite beside it goes again."""
    original = _rolled_up_month(store, "knx", {"02": [A, B, A], "03": [C]})
    first_rows = compactor._first_rows

    def first_rows_losing_c(
        batches: Iterator[tuple[str, pa.RecordBatch]], month: str
    ) -> Iterator[tuple[str, pa.RecordBatch]]:
        for day, batch in first_rows(batches, month):
            yield day, batch.slice(0, 0) if day == "03" else batch

    monkeypatch.setattr(compactor, "_first_rows", first_rows_losing_c)

    with pytest.raises(RuntimeError, match="row 4 of the original"):
        _dedupe(store.client, "knx", "2026/05")

    assert store.keys() == ["knx/2026/05/monthly.parquet", "knx/2026/05/monthly.parquet.orig"]
    assert store.table("knx/2026/05/monthly.parquet").equals(original)
    assert store.table("knx/2026/05/monthly.parquet.orig").equals(original)


def test_a_repeat_written_the_next_day_goes_too(store: Store) -> None:
    """A copy chain running past midnight lands in the next day's file."""
    _rolled_up_month(store, "knx", {"02": [A, B], "03": [B, C]})

    _dedupe(store.client, "knx", "2026/05")

    assert _rows(store.table("knx/2026/05/monthly.parquet")) == [A, B, C]


def test_repeats_after_the_redelivery_days_stay(store: Store) -> None:
    """Only 1 to 27 May 2026 had copies, so no other day loses a row."""
    _rolled_up_month(store, "knx", {"27": [A, A], "28": [C, C]})

    _dedupe(store.client, "knx", "2026/05")

    assert _rows(store.table("knx/2026/05/monthly.parquet")) == [A, C, C]


def test_a_month_in_the_old_column_order_only_gets_the_archive_order(store: Store) -> None:
    """The April 2026 monthly files have time, subject, payload; the archive writes the reverse."""
    for day, rows in {"01": [A], "02": [B, B]}.items():
        table = _archive_table(rows).select(["time", "subject", "payload"])
        store.put_table(f"knx/2026/04/{day}/daily.parquet", table)
    _rollup(store.client, "knx", "2026/04")

    _dedupe(store.client, "knx", "2026/04")

    monthly = store.table("knx/2026/04/monthly.parquet")
    assert monthly.schema.names == ["payload", "subject", "time"]
    assert _rows(monthly) == [A, B, B]


def test_a_month_still_in_daily_files_is_left_alone(store: Store) -> None:
    store.put_table("knx/2026/05/02/daily.parquet", _archive_table([A, A]))
    seeded = store.keys()

    with pytest.raises(ClientError):
        _dedupe(store.client, "knx", "2026/05")

    assert store.keys() == seeded


def test_the_switch_dedupes_only_the_named_streams_and_nothing_else(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No compaction or rollup runs beside it, and a stream not named keeps its repeats."""
    _rolled_up_month(store, "knx", {"02": [A, A]})
    _rolled_up_month(store, "solaredge_inverter", {"02": [A, A]})
    yesterday = (dt.datetime.now(dt.UTC).date() - dt.timedelta(days=1)).strftime("%Y/%m/%d")
    store.put_parquet(f"knx/{yesterday}/07/a.parquet", rows=1)
    store.put_parquet("knx/2026/08/01/daily.parquet", rows=1)
    monkeypatch.setenv("COMPACT_STREAMS", "knx")
    monkeypatch.setenv("COMPACT_DEDUPE_MONTH", "2026/05")
    monkeypatch.setenv("COMPACT_ROLLUP_MONTHS", "1")

    compactor.main()

    assert _rows(store.table("knx/2026/05/monthly.parquet")) == [A]
    assert _rows(store.table("solaredge_inverter/2026/05/monthly.parquet")) == [A, A]
    assert f"knx/{yesterday}/07/a.parquet" in store.keys()
    assert "knx/2026/08/01/daily.parquet" in store.keys()


def test_the_switch_without_named_streams_touches_nothing(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Discovery would include ems_esp and warp_*, whose May needs more than exact repeats."""
    _rolled_up_month(store, "knx", {"02": [A, A]})
    seeded = store.keys()
    monkeypatch.setenv("COMPACT_DEDUPE_MONTH", "2026/05")

    with pytest.raises(SystemExit):
        compactor.main()

    assert store.keys() == seeded


def test_rows_differing_only_in_time_stay(store: Store) -> None:
    """Only identical rows are repeats; a redelivery stamped with its own time is not."""
    later = ("2026-05-02T10:00:30.1Z", *A[1:])
    _rolled_up_month(store, "ems_esp", {"02": [A, later]})

    _dedupe(store.client, "ems_esp", "2026/05")

    assert _rows(store.table("ems_esp/2026/05/monthly.parquet")) == [A, later]
