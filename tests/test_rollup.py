from __future__ import annotations

import datetime as dt
from collections.abc import Iterator

import pyarrow as pa
import pytest

import nats_archive_compactor.__main__ as compactor
from nats_archive_compactor.__main__ import _footer, _last_month, _pending_months, _rollup

from .support import Store


def _daily_month(store: Store) -> list[str]:
    """Two days of August with a leftover day marker; returns the keys as seeded."""
    store.put_parquet("knx/2026/08/01/daily.parquet", rows=3)
    store.put_parquet("knx/2026/08/02/daily.parquet", rows=4)
    store.put("knx/2026/08/01/", b"")
    return store.keys()


def test_a_rolled_up_month_holds_only_its_monthly_file(store: Store) -> None:
    """Every row of every day lands in monthly.parquet; day and hour markers go too."""
    _daily_month(store)
    store.put_parquet("knx/2026/08/31/daily.parquet", rows=5)
    store.put("knx/2026/08/02/07/", b"")

    assert _rollup(store.client, "knx", "2026/08") == "rolled-up"

    assert store.keys() == ["knx/2026/08/monthly.parquet"]
    subjects = store.table("knx/2026/08/monthly.parquet").column("subject").to_pylist()
    assert sorted(subjects) == sorted(
        [f"knx/2026/08/01/daily.parquet.{i}" for i in range(3)]
        + [f"knx/2026/08/02/daily.parquet.{i}" for i in range(4)]
        + [f"knx/2026/08/31/daily.parquet.{i}" for i in range(5)]
    )


@pytest.mark.parametrize(
    ("today", "last_month"),
    [
        (dt.date(2026, 10, 25), "2026/09"),
        (dt.date(2026, 11, 1), "2026/10"),
        (dt.date(2026, 1, 15), "2025/12"),
    ],
)
def test_last_month_is_the_one_before_today(today: dt.date, last_month: str) -> None:
    assert _last_month(today) == last_month


def test_a_month_waits_until_the_month_after_it_is_over(store: Store) -> None:
    for month in ("2026/07", "2026/08", "2026/09", "2026/10"):
        store.put(f"knx/{month}/01/daily.parquet")
    assert _pending_months(store.client, "knx", before="2026/09") == ["2026/07", "2026/08"]


def test_a_rolled_up_month_is_not_pending(store: Store) -> None:
    store.put("knx/2026/06/monthly.parquet")
    store.put("knx/2026/07/01/daily.parquet")
    assert _pending_months(store.client, "knx", before="2026/09") == ["2026/07"]


def test_pending_months_are_oldest_first(store: Store) -> None:
    for month in ("2025/12", "2026/06", "2026/05"):
        store.put(f"knx/{month}/01/daily.parquet")
    assert _pending_months(store.client, "knx", before="2026/09") == [
        "2025/12",
        "2026/05",
        "2026/06",
    ]


def test_a_rollup_that_loses_rows_deletes_nothing(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A monthly file short of rows is dropped again, and every daily file stays."""
    seeded = _daily_month(store)
    batches = compactor._batches

    def batches_dropping_a_row(client: object, key: str) -> Iterator[pa.RecordBatch]:
        rest = batches(store.client, key)
        yield next(rest).slice(1)
        yield from rest

    monkeypatch.setattr(compactor, "_batches", batches_dropping_a_row)

    with pytest.raises(RuntimeError, match="rows"):
        _rollup(store.client, "knx", "2026/08")

    assert store.keys() == seeded


def test_a_rollup_cut_off_while_deleting_is_finished_by_the_next_run(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The monthly file records each day's rows, so the days left over can be checked."""
    _daily_month(store)
    delete = compactor._delete_keys

    def delete_one_then_fail(client: object, keys: list[str]) -> None:
        delete(store.client, keys[:1])
        raise RuntimeError("storage went away")

    monkeypatch.setattr(compactor, "_delete_keys", delete_one_then_fail)
    with pytest.raises(RuntimeError, match="went away"):
        _rollup(store.client, "knx", "2026/08")
    monkeypatch.setattr(compactor, "_delete_keys", delete)

    assert _rollup(store.client, "knx", "2026/08") == "rolled-up"

    assert store.keys() == ["knx/2026/08/monthly.parquet"]
    assert store.table("knx/2026/08/monthly.parquet").num_rows == 7


def test_a_monthly_file_missing_a_day_deletes_nothing(store: Store) -> None:
    """A day the monthly file does not account for keeps the month, monthly file included."""
    _daily_month(store)
    compactor._write_monthly(
        store.client,
        "knx/2026/08/monthly.parquet",
        {"01": "knx/2026/08/01/daily.parquet", "02": "knx/2026/08/02/daily.parquet"},
    )
    store.put_parquet("knx/2026/08/03/daily.parquet", rows=2)
    seeded = store.keys()

    with pytest.raises(RuntimeError, match="day 03"):
        _rollup(store.client, "knx", "2026/08")

    assert store.keys() == seeded


def test_a_monthly_file_without_recorded_rows_deletes_nothing(store: Store) -> None:
    _daily_month(store)
    store.put_parquet("knx/2026/08/monthly.parquet", rows=7)
    seeded = store.keys()

    with pytest.raises(RuntimeError, match="records no daily rows"):
        _rollup(store.client, "knx", "2026/08")

    assert store.keys() == seeded


def test_a_month_with_an_unmerged_hour_file_is_left_alone(store: Store) -> None:
    """Only daily files and empty markers may go; anything else blocks the month."""
    _daily_month(store)
    store.put_parquet("knx/2026/08/02/23/late.parquet", rows=1)
    seeded = store.keys()

    with pytest.raises(RuntimeError, match="knx/2026/08/02/23/late.parquet"):
        _rollup(store.client, "knx", "2026/08")

    assert store.keys() == seeded


def test_a_month_left_with_only_markers_is_swept(store: Store) -> None:
    store.put_parquet("knx/2026/08/monthly.parquet", rows=7)
    store.put("knx/2026/08/01/", b"")

    assert _rollup(store.client, "knx", "2026/08") == "already-rolled-up"

    assert store.keys() == ["knx/2026/08/monthly.parquet"]


def _archive_table(rows: int, *, nullable: bool) -> pa.Table:
    """The archive's three string columns, nullable as DuckDB wrote them or not null as pyarrow."""
    schema = pa.schema(
        [pa.field(name, pa.string(), nullable=nullable) for name in ("payload", "subject", "time")]
    )
    return pa.table({name: [f"{name}.{i}" for i in range(rows)] for name in schema.names}, schema)


def test_a_day_with_nullable_columns_rolls_up_with_the_rest(store: Store) -> None:
    """1 May 2026 was written by DuckDB with nullable columns, the rest of May not null."""
    store.put_table("knx/2026/05/01/daily.parquet", _archive_table(3, nullable=True))
    store.put_table("knx/2026/05/02/daily.parquet", _archive_table(4, nullable=False))

    assert _rollup(store.client, "knx", "2026/05") == "rolled-up"

    assert store.keys() == ["knx/2026/05/monthly.parquet"]
    assert store.table("knx/2026/05/monthly.parquet").num_rows == 7


def test_a_day_with_its_columns_in_another_order_rolls_up_in_the_newest_order(
    store: Store,
) -> None:
    """warp_charge_tracker's 1 May 2026 was made by hand with its columns in another order."""
    first = _archive_table(3, nullable=True).select(["time", "subject", "payload"])
    store.put_table("warp_charge_tracker/2026/05/01/daily.parquet", first)
    store.put_table(
        "warp_charge_tracker/2026/05/02/daily.parquet", _archive_table(4, nullable=False)
    )

    assert _rollup(store.client, "warp_charge_tracker", "2026/05") == "rolled-up"

    monthly = store.table("warp_charge_tracker/2026/05/monthly.parquet")
    assert monthly.schema.names == ["payload", "subject", "time"]
    assert sorted(monthly.column("time").to_pylist()) == sorted(
        [f"time.{i}" for i in range(3)] + [f"time.{i}" for i in range(4)]
    )


def test_a_day_larger_than_a_batch_is_read_in_pieces(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No day is held whole: every batch becomes a row group of its own."""
    monkeypatch.setattr(compactor, "ROLLUP_BATCH_ROWS", 2)
    _daily_month(store)

    assert _rollup(store.client, "knx", "2026/08") == "rolled-up"

    monthly = _footer(store.client, "knx/2026/08/monthly.parquet")
    assert (monthly.num_rows, monthly.num_row_groups) == (7, 4)


def test_a_month_with_two_schemas_deletes_nothing(store: Store) -> None:
    """Daily files must share one schema; the half-written monthly file goes again."""
    store.put_table("ems_esp/2026/08/01/daily.parquet", pa.table({"subject": ["s1", "s2"]}))
    store.put_table(
        "ems_esp/2026/08/02/daily.parquet", pa.table({"subject": ["s3"], "unit": ["°C"]})
    )
    seeded = store.keys()

    with pytest.raises(ValueError, match="schema"):
        _rollup(store.client, "ems_esp", "2026/08")

    assert store.keys() == seeded
