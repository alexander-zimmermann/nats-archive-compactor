from __future__ import annotations

import pytest

from nats_archive_compactor.__main__ import STREAMS, _resolve_streams


def test_default_returns_all_known(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("COMPACT_STREAMS", raising=False)
    assert _resolve_streams() == STREAMS


def test_subset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COMPACT_STREAMS", "knx, ems_esp")
    assert _resolve_streams() == ("knx", "ems_esp")


def test_unknown_stream_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COMPACT_STREAMS", "knx,bogus")
    with pytest.raises(ValueError, match="bogus"):
        _resolve_streams()


def test_blank_string_falls_back_to_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COMPACT_STREAMS", "   ")
    assert _resolve_streams() == STREAMS
