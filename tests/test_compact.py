from __future__ import annotations

import pyarrow as pa
import pytest

import nats_archive_compactor.__main__ as compactor
from nats_archive_compactor.__main__ import _compact, _delete_keys

from .support import Store


def test_a_compacted_day_holds_only_its_daily_file(store: Store) -> None:
    """Every row lands in daily.parquet, and no empty marker is left for the day or its hours."""
    store.put_parquet("knx/2026/10/18/07/a.parquet", rows=3)
    store.put_parquet("knx/2026/10/18/07/b.parquet", rows=2)
    store.put_parquet("knx/2026/10/18/08/c.parquet", rows=4)

    assert _compact(store.client, "knx", "2026/10/18") == "compacted"

    assert store.keys() == ["knx/2026/10/18/daily.parquet"]
    assert store.table("knx/2026/10/18/daily.parquet").num_rows == 9


def test_a_file_written_while_merging_is_left_in_place(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The delete takes the merged keys, not whatever the day holds by then."""
    store.put_parquet("knx/2026/10/18/07/a.parquet", rows=3)
    read = compactor._read_table

    def read_while_a_late_file_lands(client: object, keys: list[str]) -> pa.Table:
        store.put_parquet("knx/2026/10/18/23/late.parquet", rows=1)
        return read(store.client, keys)

    monkeypatch.setattr(compactor, "_read_table", read_while_a_late_file_lands)

    _compact(store.client, "knx", "2026/10/18")

    assert store.keys() == ["knx/2026/10/18/23/late.parquet", "knx/2026/10/18/daily.parquet"]
    assert store.table("knx/2026/10/18/daily.parquet").num_rows == 3


def test_a_day_with_two_schemas_is_left_alone(store: Store) -> None:
    """Hour files must share one schema; otherwise nothing is written or deleted."""
    store.put_table("ems_esp/2026/10/18/07/a.parquet", pa.table({"subject": ["s1", "s2"]}))
    store.put_table(
        "ems_esp/2026/10/18/08/b.parquet", pa.table({"subject": ["s3"], "unit": ["°C"]})
    )
    seeded = store.keys()

    with pytest.raises(pa.ArrowInvalid):
        _compact(store.client, "ems_esp", "2026/10/18")

    assert store.keys() == seeded


def test_a_key_already_gone_does_not_stop_the_rest(store: Store) -> None:
    """A retried batch re-deletes keys the previous attempt removed."""
    store.put("knx/2026/10/18/08/b.parquet")

    _delete_keys(store.client, ["knx/2026/10/18/07/a.parquet", "knx/2026/10/18/08/b.parquet"])

    assert store.keys() == []
