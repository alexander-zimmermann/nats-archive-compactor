from __future__ import annotations

from dataclasses import dataclass

import pyarrow.fs as fs
import pytest

from nats_archive_compactor.__main__ import (
    _delete_merged,
    _pending_days,
    _resolve_streams,
    _stream_days,
    _targets,
)


@dataclass(frozen=True)
class _Info:
    path: str
    type: fs.FileType


class FakeS3:
    """Minimal stand-in for S3FileSystem: a set of object keys plus derived directories.

    Only the two call shapes the compactor uses are supported — get_file_info with a
    path (does this object exist) and with a FileSelector (what is directly below).
    """

    def __init__(self, keys: set[str]) -> None:
        self.keys = keys
        self.deleted_files: list[str] = []

    def delete_file(self, key: str) -> None:
        if key not in self.keys:
            raise FileNotFoundError(key)
        self.keys.discard(key)
        self.deleted_files.append(key)

    def delete_dir(self, path: str) -> None:
        raise AssertionError(f"compaction must not delete directories, got {path}")

    def get_file_info(self, arg: str | fs.FileSelector) -> _Info | list[_Info]:
        if isinstance(arg, str):
            kind = fs.FileType.File if arg in self.keys else fs.FileType.NotFound
            return _Info(arg, kind)

        base = arg.base_dir.rstrip("/")
        children: dict[str, fs.FileType] = {}
        for key in self.keys:
            if not key.startswith(f"{base}/"):
                continue
            rest = key[len(base) + 1 :].split("/")
            kind = fs.FileType.File if len(rest) == 1 else fs.FileType.Directory
            children[f"{base}/{rest[0]}"] = kind
        return [_Info(path, kind) for path, kind in sorted(children.items())]


def _keys(*paths: str) -> set[str]:
    return {f"nats-archive/{p}" for p in paths}


def test_streams_are_discovered_from_the_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("COMPACT_STREAMS", raising=False)
    s3 = FakeS3(_keys("knx/2026/10/01/daily.parquet", "dyson/2026/10/01/09/a.parquet"))
    assert _resolve_streams(s3) == ("dyson", "knx")


def test_override_wins_over_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COMPACT_STREAMS", "knx, dyson")
    s3 = FakeS3(_keys("knx/2026/10/01/daily.parquet"))
    assert _resolve_streams(s3) == ("knx", "dyson")


def test_blank_override_falls_back_to_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COMPACT_STREAMS", "   ")
    s3 = FakeS3(_keys("knx/2026/10/01/daily.parquet"))
    assert _resolve_streams(s3) == ("knx",)


def test_stream_days_newest_first() -> None:
    s3 = FakeS3(
        _keys(
            "dyson/2026/09/30/09/a.parquet",
            "dyson/2026/10/01/09/a.parquet",
            "dyson/2026/10/02/09/a.parquet",
        )
    )
    assert _stream_days(s3, "dyson") == ["2026/10/02", "2026/10/01", "2026/09/30"]


def test_pending_skips_compacted_and_today() -> None:
    s3 = FakeS3(
        _keys(
            "dyson/2026/10/01/daily.parquet",
            "dyson/2026/10/02/09/a.parquet",
            "dyson/2026/10/03/09/a.parquet",
        )
    )
    assert _pending_days(s3, "dyson", today="2026/10/03", limit=10) == ["2026/10/02"]


def test_pending_respects_the_limit() -> None:
    s3 = FakeS3(
        _keys(*[f"dyson/2026/09/{d:02d}/09/a.parquet" for d in range(1, 11)]),
    )
    assert _pending_days(s3, "dyson", today="2026/10/01", limit=3) == [
        "2026/09/10",
        "2026/09/09",
        "2026/09/08",
    ]


def test_targets_without_backfill_is_the_single_day() -> None:
    s3 = FakeS3(_keys("dyson/2026/09/01/09/a.parquet"))
    assert _targets(s3, "dyson", day="2026/09/30", today="2026/10/01", backfill=0) == ["2026/09/30"]


def test_targets_with_backfill_returns_pending_days() -> None:
    s3 = FakeS3(
        _keys("dyson/2026/09/29/09/a.parquet", "dyson/2026/09/30/09/a.parquet"),
    )
    assert _targets(s3, "dyson", day="2026/09/30", today="2026/10/01", backfill=5) == [
        "2026/09/30",
        "2026/09/29",
    ]


def test_only_the_merged_keys_are_deleted() -> None:
    """The delete follows what was merged, not a fresh listing of the day."""
    s3 = FakeS3(
        _keys(
            "knx/2026/10/18/07/a.parquet",
            "knx/2026/10/18/08/b.parquet",
            "knx/2026/10/18/daily.parquet",
            "knx/2026/10/19/07/c.parquet",
        )
    )
    merged = sorted(_keys("knx/2026/10/18/07/a.parquet", "knx/2026/10/18/08/b.parquet"))

    _delete_merged(s3, merged)

    assert sorted(s3.deleted_files) == merged
    # The daily file and an unrelated day survive; a late write would too, because it
    # was never in `merged`.
    assert s3.keys == _keys("knx/2026/10/18/daily.parquet", "knx/2026/10/19/07/c.parquet")


def test_a_key_already_gone_does_not_stop_the_rest() -> None:
    """A retried batch re-deletes keys the previous attempt removed."""
    s3 = FakeS3(_keys("knx/2026/10/18/08/b.parquet"))
    missing = next(iter(_keys("knx/2026/10/18/07/a.parquet")))
    present = next(iter(_keys("knx/2026/10/18/08/b.parquet")))

    _delete_merged(s3, [missing, present])

    assert s3.deleted_files == [present]
    assert s3.keys == set()
