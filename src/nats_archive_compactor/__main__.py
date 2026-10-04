from __future__ import annotations

import datetime as dt
import os
import sys
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

BUCKET = "nats-archive"

DEFAULT_REQUEST_TIMEOUT = 60.0
DEFAULT_CONNECT_TIMEOUT = 10.0
RETRY_ATTEMPTS = 3
RETRY_WAIT_MIN = 2
RETRY_WAIT_MAX = 16

log = structlog.get_logger()

_retry = retry(
    reraise=True,
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX),
    retry=retry_if_exception_type(OSError),
)


def _build_s3() -> fs.S3FileSystem:
    url = urlparse(os.environ["RUSTFS_URL"])
    return fs.S3FileSystem(
        endpoint_override=f"{url.hostname}:{url.port or 9000}",
        scheme=url.scheme,
        access_key=os.environ["ACCESS_KEY"],
        secret_key=os.environ["SECRET_KEY"],
        force_virtual_addressing=False,
        request_timeout=float(os.environ.get("S3_REQUEST_TIMEOUT", DEFAULT_REQUEST_TIMEOUT)),
        connect_timeout=float(os.environ.get("S3_CONNECT_TIMEOUT", DEFAULT_CONNECT_TIMEOUT)),
    )


@_retry
def _subdirs(s3: fs.S3FileSystem, prefix: str) -> list[str]:
    """Return the names of the directories directly under `prefix`, sorted."""
    selector = fs.FileSelector(prefix, recursive=False, allow_not_found=True)
    return sorted(
        entry.path.rsplit("/", 1)[-1]
        for entry in s3.get_file_info(selector)
        if entry.type == fs.FileType.Directory
    )


def _resolve_streams(s3: fs.S3FileSystem) -> tuple[str, ...]:
    """Streams are the top-level prefixes in the bucket, or an explicit override.

    Discovering them means a stream added by a new sidecar is compacted without a
    code change; a hardcoded list silently skipped seven of them for months.
    """
    raw = os.environ.get("COMPACT_STREAMS", "").strip()
    if raw:
        return tuple(s.strip() for s in raw.split(",") if s.strip())
    return tuple(_subdirs(s3, BUCKET))


def _stream_days(s3: fs.S3FileSystem, stream: str) -> list[str]:
    """Return every YYYY/MM/DD prefix present for a stream, newest first."""
    base = f"{BUCKET}/{stream}"
    days: list[str] = []
    for year in _subdirs(s3, base):
        for month in _subdirs(s3, f"{base}/{year}"):
            days.extend(f"{year}/{month}/{day}" for day in _subdirs(s3, f"{base}/{year}/{month}"))
    return sorted(days, reverse=True)


@_retry
def _is_compacted(s3: fs.S3FileSystem, stream: str, day: str) -> bool:
    info = s3.get_file_info(f"{BUCKET}/{stream}/{day}/daily.parquet")
    return bool(info.type == fs.FileType.File)


def _pending_days(s3: fs.S3FileSystem, stream: str, today: str, limit: int) -> list[str]:
    """Up to `limit` days for this stream that still have no daily.parquet, newest first.

    Today is skipped because its hours are still being written. Walking newest-first
    stops after `limit` hits, so an already-compacted stream costs a handful of probes.
    """
    pending: list[str] = []
    for day in _stream_days(s3, stream):
        if day >= today:
            continue
        if not _is_compacted(s3, stream, day):
            pending.append(day)
            if len(pending) >= limit:
                break
    return pending


@_retry
def _list_hour_parquets(s3: fs.S3FileSystem, day_prefix: str) -> list[str]:
    """List *.parquet under <day_prefix>/HH/, hour-by-hour to keep each call small.

    A single recursive list over a day with a burst hour (thousands of files) can
    exceed the S3 client's slow-transfer threshold; iterating per hour keeps each
    response well under that bound.
    """
    sources: list[str] = []
    for hh in range(24):
        prefix = f"{day_prefix}/{hh:02d}"
        selector = fs.FileSelector(prefix, recursive=False, allow_not_found=True)
        sources.extend(f.path for f in s3.get_file_info(selector) if f.path.endswith(".parquet"))
    return sources


@_retry
def _read_table(s3: fs.S3FileSystem, sources: list[str]) -> pa.Table:
    return pq.read_table(sources, filesystem=s3)


@_retry
def _write_daily(s3: fs.S3FileSystem, table: pa.Table, daily: str) -> None:
    pq.write_table(table, daily, filesystem=s3, compression="zstd")


def _compact(s3: fs.S3FileSystem, stream: str, day: str) -> str:
    """Merge stream's hour-files for `day` into one daily.parquet. Return status string."""
    day_prefix = f"{BUCKET}/{stream}/{day}"
    daily = f"{day_prefix}/daily.parquet"

    if s3.get_file_info(daily).type == fs.FileType.File:
        return "already-compacted"

    sources = _list_hour_parquets(s3, day_prefix)
    if not sources:
        return "no-source-files"

    table = _read_table(s3, sources)
    _write_daily(s3, table, daily)

    if s3.get_file_info(daily).type != fs.FileType.File:
        raise RuntimeError("daily.parquet missing after write")

    for entry in s3.get_file_info(fs.FileSelector(day_prefix)):
        if entry.type == fs.FileType.Directory:
            s3.delete_dir(entry.path)

    return "compacted"


def _targets(s3: fs.S3FileSystem, stream: str, day: str, today: str, backfill: int) -> list[str]:
    """The days to compact for this stream: the single day, or a bounded backfill."""
    if backfill <= 0:
        return [day]
    return _pending_days(s3, stream, today, backfill)


def main() -> None:
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )

    now = dt.datetime.now(dt.UTC).date()
    today = now.strftime("%Y/%m/%d")
    explicit_day = os.environ.get("COMPACT_DAY")
    day = explicit_day or (now - dt.timedelta(days=1)).strftime("%Y/%m/%d")
    # An explicit day names exactly one target, so backfill only applies without it.
    backfill = 0 if explicit_day else int(os.environ.get("COMPACT_BACKFILL_DAYS", "0"))

    s3 = _build_s3()
    streams = _resolve_streams(s3)
    log.info("compaction.start", day=day, backfill_days=backfill, streams=list(streams))

    failed: list[str] = []
    compacted = 0

    for stream in streams:
        try:
            targets = _targets(s3, stream, day, today, backfill)
        except Exception as exc:  # pyarrow raises a wide net of OSError-likes
            log.error("compaction.stream", stream=stream, status="list-failed", error=str(exc))
            failed.append(stream)
            continue

        for target in targets:
            try:
                status = _compact(s3, stream, target)
                if status == "compacted":
                    compacted += 1
                log.info("compaction.stream", stream=stream, day=target, status=status)
            except Exception as exc:
                log.error(
                    "compaction.stream",
                    stream=stream,
                    day=target,
                    status="failed",
                    error=str(exc),
                )
                failed.append(f"{stream}@{target}")

    if failed:
        log.warning("compaction.done", day=day, compacted=compacted, failed=failed)
        sys.exit(1)
    log.info("compaction.done", day=day, compacted=compacted)


if __name__ == "__main__":
    main()
