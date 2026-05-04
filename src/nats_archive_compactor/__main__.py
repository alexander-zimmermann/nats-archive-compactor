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

STREAMS = (
    "knx",
    "ems_esp",
    "solaredge_inverter",
    "solaredge_powerflow",
    "warp_system",
    "warp_evse",
    "warp_charge_manager",
    "warp_charge_tracker",
    "warp_meter",
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


def _resolve_streams() -> tuple[str, ...]:
    raw = os.environ.get("COMPACT_STREAMS", "").strip()
    if not raw:
        return STREAMS
    requested = tuple(s.strip() for s in raw.split(",") if s.strip())
    unknown = [s for s in requested if s not in STREAMS]
    if unknown:
        raise ValueError(f"Unknown streams in COMPACT_STREAMS: {unknown}")
    return requested


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
        sources.extend(
            f.path
            for f in s3.get_file_info(selector)
            if f.path.endswith(".parquet")
        )
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


def main() -> None:
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )

    yesterday = dt.datetime.now(dt.UTC).date() - dt.timedelta(days=1)
    day = os.environ.get("COMPACT_DAY") or yesterday.strftime("%Y/%m/%d")
    streams = _resolve_streams()
    log.info("compaction.start", day=day, streams=list(streams))

    s3 = _build_s3()
    failed: list[str] = []

    for stream in streams:
        try:
            status = _compact(s3, stream, day)
            log.info("compaction.stream", stream=stream, status=status)
        except Exception as exc:  # pyarrow raises a wide net of OSError-likes
            log.error("compaction.stream", stream=stream, status="failed", error=str(exc))
            failed.append(stream)

    if failed:
        log.warning("compaction.done", day=day, failed=failed)
        sys.exit(1)
    log.info("compaction.done", day=day)


if __name__ == "__main__":
    main()
