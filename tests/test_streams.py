from __future__ import annotations

import pytest

from nats_archive_compactor.__main__ import (
    _pending_days,
    _resolve_streams,
    _stream_days,
    _targets,
)

from .support import Store


def test_streams_are_discovered_from_the_bucket(store: Store) -> None:
    store.put("knx/2026/10/01/daily.parquet")
    store.put("dyson/2026/10/01/09/a.parquet")
    assert _resolve_streams(store.client) == ("dyson", "knx")


def test_override_wins_over_discovery(store: Store, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COMPACT_STREAMS", "knx, dyson")
    store.put("knx/2026/10/01/daily.parquet")
    assert _resolve_streams(store.client) == ("knx", "dyson")


def test_blank_override_falls_back_to_discovery(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("COMPACT_STREAMS", "   ")
    store.put("knx/2026/10/01/daily.parquet")
    assert _resolve_streams(store.client) == ("knx",)


def test_stream_days_newest_first(store: Store) -> None:
    store.put("dyson/2026/09/30/09/a.parquet")
    store.put("dyson/2026/10/01/09/a.parquet")
    store.put("dyson/2026/10/02/09/a.parquet")
    assert _stream_days(store.client, "dyson") == ["2026/10/02", "2026/10/01", "2026/09/30"]


def test_pending_skips_compacted_and_today(store: Store) -> None:
    store.put("dyson/2026/10/01/daily.parquet")
    store.put("dyson/2026/10/02/09/a.parquet")
    store.put("dyson/2026/10/03/09/a.parquet")
    assert _pending_days(store.client, "dyson", today="2026/10/03", limit=10) == ["2026/10/02"]


def test_pending_respects_the_limit(store: Store) -> None:
    for d in range(1, 11):
        store.put(f"dyson/2026/09/{d:02d}/09/a.parquet")
    assert _pending_days(store.client, "dyson", today="2026/10/01", limit=3) == [
        "2026/09/10",
        "2026/09/09",
        "2026/09/08",
    ]


def test_targets_without_backfill_is_the_single_day(store: Store) -> None:
    store.put("dyson/2026/09/01/09/a.parquet")
    assert _targets(store.client, "dyson", day="2026/09/30", today="2026/10/01", backfill=0) == [
        "2026/09/30"
    ]


def test_targets_with_backfill_returns_pending_days(store: Store) -> None:
    store.put("dyson/2026/09/29/09/a.parquet")
    store.put("dyson/2026/09/30/09/a.parquet")
    assert _targets(store.client, "dyson", day="2026/09/30", today="2026/10/01", backfill=5) == [
        "2026/09/30",
        "2026/09/29",
    ]
