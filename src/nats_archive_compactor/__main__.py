from __future__ import annotations

import datetime as dt
import os
import sys
from urllib.parse import urlparse

import pyarrow.fs as fs
import pyarrow.parquet as pq
import structlog

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

log = structlog.get_logger()


def _build_s3() -> fs.S3FileSystem:
    url = urlparse(os.environ["RUSTFS_URL"])
    return fs.S3FileSystem(
        endpoint_override=f"{url.hostname}:{url.port or 9000}",
        scheme=url.scheme,
        access_key=os.environ["ACCESS_KEY"],
        secret_key=os.environ["SECRET_KEY"],
        force_virtual_addressing=False,
    )


def _list_hour_parquets(s3: fs.S3FileSystem, prefix: str) -> list[str]:
    """Return absolute paths of *.parquet files under <prefix>/HH/, ignoring daily.parquet."""
    selector = fs.FileSelector(prefix, recursive=True, allow_not_found=True)
    return [
        f.path
        for f in s3.get_file_info(selector)
        if f.path.endswith(".parquet") and not f.path.endswith("/daily.parquet")
    ]


def _compact(s3: fs.S3FileSystem, stream: str, day: str) -> str:
    """Merge stream's hour-files for `day` into one daily.parquet. Return status string."""
    prefix = f"{BUCKET}/{stream}/{day}"
    daily = f"{prefix}/daily.parquet"

    if s3.get_file_info(daily).type == fs.FileType.File:
        return "already-compacted"

    sources = _list_hour_parquets(s3, prefix)
    if not sources:
        return "no-source-files"

    table = pq.read_table(sources, filesystem=s3)
    pq.write_table(table, daily, filesystem=s3, compression="zstd")

    # Remove the hour-folders only after the daily file is verified.
    if s3.get_file_info(daily).type != fs.FileType.File:
        raise RuntimeError("daily.parquet missing after write")

    for entry in s3.get_file_info(fs.FileSelector(prefix)):
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
    log.info("compaction.start", day=day, streams=len(STREAMS))

    s3 = _build_s3()
    failed: list[str] = []

    for stream in STREAMS:
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
