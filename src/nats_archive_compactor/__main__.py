from __future__ import annotations

import datetime as dt
import io
import os
import re
import sys
import tempfile
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from botocore.config import Config
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

BUCKET = "nats-archive"

DEFAULT_REQUEST_TIMEOUT = 60.0
DEFAULT_CONNECT_TIMEOUT = 10.0
RETRY_ATTEMPTS = 3
DELETE_BATCH = 1000  # the most keys one DeleteObjects call takes

_HOUR_FILE = re.compile(r"\d{2}/[^/]+\.parquet")

log = structlog.get_logger()


def _build_client() -> S3Client:
    """An S3 client on plain keys, so a prefix disappears with its last key."""
    url = urlparse(os.environ["RUSTFS_URL"])
    return boto3.client(
        "s3",
        endpoint_url=f"{url.scheme}://{url.hostname}:{url.port or 9000}",
        aws_access_key_id=os.environ["ACCESS_KEY"],
        aws_secret_access_key=os.environ["SECRET_KEY"],
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            connect_timeout=float(os.environ.get("S3_CONNECT_TIMEOUT", DEFAULT_CONNECT_TIMEOUT)),
            read_timeout=float(os.environ.get("S3_REQUEST_TIMEOUT", DEFAULT_REQUEST_TIMEOUT)),
            retries={"total_max_attempts": RETRY_ATTEMPTS, "mode": "standard"},
        ),
    )


def _subdirs(client: S3Client, prefix: str) -> list[str]:
    """Return the names of the prefixes directly under `prefix` ("" is the bucket), sorted."""
    base = f"{prefix}/" if prefix else ""
    pages = client.get_paginator("list_objects_v2").paginate(
        Bucket=BUCKET, Prefix=base, Delimiter="/"
    )
    return sorted(
        common["Prefix"][len(base) : -1]
        for page in pages
        for common in page.get("CommonPrefixes", [])
    )


def _list_keys(client: S3Client, prefix: str) -> list[str]:
    """Every key under `prefix`, in key order."""
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix)
    return [obj["Key"] for page in pages for obj in page.get("Contents", [])]


def _exists(client: S3Client, key: str) -> bool:
    try:
        client.head_object(Bucket=BUCKET, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "404":
            return False
        raise
    return True


def _resolve_streams(client: S3Client) -> tuple[str, ...]:
    """Streams are the top-level prefixes in the bucket, or an explicit override.

    Discovering them means a stream added by a new sidecar is compacted without a
    code change; a hardcoded list silently skipped seven of them for months.
    """
    raw = os.environ.get("COMPACT_STREAMS", "").strip()
    if raw:
        return tuple(s.strip() for s in raw.split(",") if s.strip())
    return tuple(_subdirs(client, ""))


def _stream_days(client: S3Client, stream: str) -> list[str]:
    """Return every YYYY/MM/DD prefix present for a stream, newest first."""
    days: list[str] = []
    for year in _subdirs(client, stream):
        for month in _subdirs(client, f"{stream}/{year}"):
            days.extend(
                f"{year}/{month}/{day}" for day in _subdirs(client, f"{stream}/{year}/{month}")
            )
    return sorted(days, reverse=True)


def _is_compacted(client: S3Client, stream: str, day: str) -> bool:
    return _exists(client, f"{stream}/{day}/daily.parquet")


def _pending_days(client: S3Client, stream: str, today: str, limit: int) -> list[str]:
    """Up to `limit` days for this stream that still have no daily.parquet, newest first.

    Today is skipped because its hours are still being written. Walking newest-first
    stops after `limit` hits, so an already-compacted stream costs a handful of probes.
    """
    pending: list[str] = []
    for day in _stream_days(client, stream):
        if day >= today:
            continue
        if not _is_compacted(client, stream, day):
            pending.append(day)
            if len(pending) >= limit:
                break
    return pending


def _hour_parquets(client: S3Client, day_prefix: str) -> list[str]:
    """The *.parquet keys under <day_prefix>/HH/."""
    base = f"{day_prefix}/"
    return [key for key in _list_keys(client, base) if _HOUR_FILE.fullmatch(key[len(base) :])]


def _download(client: S3Client, key: str) -> pa.Table:
    """Read one Parquet object; the transfer retries a body cut off mid-read."""
    buffer = io.BytesIO()
    client.download_fileobj(BUCKET, key, buffer)
    buffer.seek(0)
    return pq.read_table(buffer)


def _read_table(client: S3Client, keys: list[str]) -> pa.Table:
    """Read and concatenate the files, which must share one schema."""
    return pa.concat_tables([_download(client, key) for key in keys])


def _write_daily(client: S3Client, table: pa.Table, key: str) -> None:
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        pq.write_table(table, tmp.name, compression="zstd")
        client.upload_file(tmp.name, BUCKET, key)


def _delete_keys(client: S3Client, keys: list[str]) -> None:
    """Delete exactly these keys and nothing else; a key already gone counts as deleted."""
    for start in range(0, len(keys), DELETE_BATCH):
        batch = keys[start : start + DELETE_BATCH]
        response = client.delete_objects(
            Bucket=BUCKET,
            Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
        )
        errors = response.get("Errors", [])
        if errors:
            raise RuntimeError(f"{len(errors)} of {len(batch)} keys not deleted: {errors[0]}")


def _compact(client: S3Client, stream: str, day: str) -> str:
    """Merge stream's hour-files for `day` into one daily.parquet. Return status string."""
    day_prefix = f"{stream}/{day}"

    if _is_compacted(client, stream, day):
        return "already-compacted"

    sources = _hour_parquets(client, day_prefix)
    if not sources:
        return "no-source-files"

    table = _read_table(client, sources)
    _write_daily(client, table, f"{day_prefix}/daily.parquet")

    if not _is_compacted(client, stream, day):
        raise RuntimeError("daily.parquet missing after write")

    _delete_keys(client, sources)

    return "compacted"


def _targets(client: S3Client, stream: str, day: str, today: str, backfill: int) -> list[str]:
    """The days to compact for this stream: the single day, or a bounded backfill."""
    if backfill <= 0:
        return [day]
    return _pending_days(client, stream, today, backfill)


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

    client = _build_client()
    streams = _resolve_streams(client)
    log.info("compaction.start", day=day, backfill_days=backfill, streams=list(streams))

    failed: list[str] = []
    compacted = 0

    for stream in streams:
        try:
            targets = _targets(client, stream, day, today, backfill)
        except Exception as exc:  # botocore raises ClientError and a family of connection errors
            log.error("compaction.stream", stream=stream, status="list-failed", error=str(exc))
            failed.append(stream)
            continue

        for target in targets:
            try:
                status = _compact(client, stream, target)
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
