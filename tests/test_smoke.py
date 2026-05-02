from __future__ import annotations

from nats_archive_compactor import __version__


def test_version_set() -> None:
    assert __version__
