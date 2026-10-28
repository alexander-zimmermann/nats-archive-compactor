from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import io
import json
import os
import re
import sys
import tempfile
from collections import Counter
from collections.abc import Callable, Iterator
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
DAILY_ROWS = b"daily_rows"  # monthly.parquet metadata: rows taken from each day
ROLLUP_BATCH_ROWS = 16_384  # rows read and written at a time, so a day of any size fits in memory
MONTHLY_BACKUP = "monthly.parquet.orig"  # a deduplicated month's file as it was before
MONTHLY_REWRITE = "monthly.parquet.new"  # a deduplicated month's file until it is verified
ARCHIVE_COLUMNS = ("payload", "subject", "time")  # the column order the archive writes today
ROW_FIELDS = ("time", "subject", "payload")  # what makes rows identical, payload last
REDELIVERY_DAYS = ("2026/05/01", "2026/05/27")  # NATS redelivered every message every 30 s
FILE_BUFFER_BYTES = 1 << 20  # read buffer for a local Parquet file

_HOUR_FILE = re.compile(r"\d{2}/[^/]+\.parquet")
_DAILY_FILE = re.compile(r"(\d{2})/daily\.parquet")

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


def _list_objects(client: S3Client, prefix: str) -> dict[str, int]:
    """Every key under `prefix` with its size, in key order."""
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix)
    return {obj["Key"]: obj["Size"] for page in pages for obj in page.get("Contents", [])}


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


def _last_month(today: dt.date) -> str:
    """The month before today's, as YYYY/MM."""
    return (today.replace(day=1) - dt.timedelta(days=1)).strftime("%Y/%m")


def _pending_months(client: S3Client, stream: str, before: str) -> list[str]:
    """The months older than `before` that still hold day prefixes, oldest first."""
    months = sorted({day[:7] for day in _stream_days(client, stream)})
    return [month for month in months if month < before]


def _hour_parquets(client: S3Client, day_prefix: str) -> list[str]:
    """The *.parquet keys under <day_prefix>/HH/."""
    base = f"{day_prefix}/"
    return [key for key in _list_objects(client, base) if _HOUR_FILE.fullmatch(key[len(base) :])]


def _fetch(client: S3Client, key: str) -> io.BytesIO:
    """One object's body; the transfer retries a body cut off mid-read."""
    buffer = io.BytesIO()
    client.download_fileobj(BUCKET, key, buffer)
    buffer.seek(0)
    return buffer


def _download(client: S3Client, key: str) -> pa.Table:
    """Read one Parquet object whole."""
    return pq.read_table(_fetch(client, key))


def _batches(client: S3Client, key: str) -> Iterator[pa.RecordBatch]:
    """Read one Parquet object in batches of ROLLUP_BATCH_ROWS rows."""
    yield from pq.ParquetFile(_fetch(client, key)).iter_batches(batch_size=ROLLUP_BATCH_ROWS)


def _read_table(client: S3Client, keys: list[str]) -> pa.Table:
    """Read and concatenate the files, which must share one schema."""
    return pa.concat_tables([_download(client, key) for key in keys])


def _write_daily(client: S3Client, table: pa.Table, key: str) -> None:
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        pq.write_table(table, tmp.name, compression="zstd")
        client.upload_file(tmp.name, BUCKET, key)


def _copy(client: S3Client, source: str, key: str) -> None:
    """Copy one object to `key` inside the bucket, without downloading it."""
    client.copy_object(Bucket=BUCKET, Key=key, CopySource={"Bucket": BUCKET, "Key": source})


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


def _footer(client: S3Client, key: str) -> pq.FileMetaData:
    """An object's Parquet footer, fetched with two ranged reads."""
    tail = client.get_object(Bucket=BUCKET, Key=key, Range="bytes=-8")["Body"].read()
    length = int.from_bytes(tail[:4], "little")
    footer = client.get_object(Bucket=BUCKET, Key=key, Range=f"bytes=-{length + 8}")
    return pq.read_metadata(pa.BufferReader(footer["Body"].read()))


def _recorded_days(footer: pq.FileMetaData) -> dict[str, int]:
    """The rows a monthly file records for each of its days, in file order."""
    if not footer.metadata or DAILY_ROWS not in footer.metadata:
        raise RuntimeError("monthly.parquet records no daily rows")
    days: dict[str, int] = json.loads(footer.metadata[DAILY_ROWS])
    return days


def _conform(batch: pa.RecordBatch, schema: pa.Schema) -> pa.RecordBatch:
    """The batch in `schema`, whose column names and types it must share.

    Only order and nullability may differ: DuckDB wrote the first daily files (1 May 2026)
    with nullable columns, pyarrow writes them not null, and warp_charge_tracker's 1 May was
    made by hand with its columns in another order. A null the schema forbids fails the cast.
    """
    if sorted(batch.schema.names) == sorted(schema.names):
        batch = batch.select(schema.names)
    if [(f.name, f.type) for f in batch.schema] != [(f.name, f.type) for f in schema]:
        raise ValueError(f"schema {batch.schema} does not match {schema}")
    return batch.cast(schema)


def _write_monthly(client: S3Client, key: str, dailies: dict[str, str]) -> None:
    """Stream the daily files, which must share one schema, into one Parquet object.

    The newest day sets the schema, so the file gets the column order the archive writes today.
    """
    days = sorted(dailies.items())
    schema = _footer(client, days[-1][1]).schema.to_arrow_schema()
    rows: dict[str, int] = {}
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        with pq.ParquetWriter(tmp.name, schema, compression="zstd") as writer:
            for day, daily in days:
                rows[day] = 0
                for batch in _batches(client, daily):
                    writer.write_batch(_conform(batch, schema))
                    rows[day] += batch.num_rows
            writer.add_key_value_metadata({DAILY_ROWS: json.dumps(rows)})
        client.upload_file(tmp.name, BUCKET, key)


def _verify_monthly(client: S3Client, key: str, dailies: dict[str, str]) -> None:
    """Raise unless monthly.parquet holds exactly the rows recorded for every daily file left."""
    footer = _footer(client, key)
    recorded = _recorded_days(footer)
    if footer.num_rows != sum(recorded.values()):
        raise RuntimeError(f"monthly.parquet holds {footer.num_rows} rows, recorded {recorded}")
    for day, daily in sorted(dailies.items()):
        rows = _footer(client, daily).num_rows
        if recorded.get(day) != rows:
            raise RuntimeError(f"day {day} has {rows} rows, monthly.parquet {recorded.get(day)}")


def _rollup(client: S3Client, stream: str, month: str) -> str:
    """Merge the month's daily files into monthly.parquet, verify, then delete them."""
    prefix = f"{stream}/{month}/"
    monthly = f"{prefix}monthly.parquet"
    objects = _list_objects(client, prefix)
    dailies = {
        match[1]: key for key in objects if (match := _DAILY_FILE.fullmatch(key[len(prefix) :]))
    }
    markers = [key for key, size in objects.items() if key.endswith("/") and size == 0]
    strays = sorted(objects.keys() - {monthly, *dailies.values(), *markers})
    if strays:
        raise RuntimeError(f"{len(strays)} keys besides daily files, first {strays[0]}")

    if not dailies:
        _delete_keys(client, markers)
        return "already-rolled-up" if monthly in objects else "no-daily-files"

    fresh = monthly not in objects
    try:
        if fresh:
            _write_monthly(client, monthly, dailies)
        _verify_monthly(client, monthly, dailies)
    except Exception:
        if fresh:
            # Every daily file is still in place, so the unverified monthly file goes.
            _delete_keys(client, [monthly])
        raise

    _delete_keys(client, [*dailies.values(), *markers])
    return "rolled-up"


@contextlib.contextmanager
def _downloaded(client: S3Client, key: str) -> Iterator[str]:
    """The object in a temporary local file, so it is read from disk rather than held in memory."""
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        client.download_file(BUCKET, key, tmp.name)
        yield tmp.name


def _day_batches(path: str, days: dict[str, int]) -> Iterator[tuple[str, pa.RecordBatch]]:
    """A local monthly file's rows in batches, each cut to lie within one of its recorded days.

    Without pre-buffering, pyarrow reads one batch at a time instead of the whole file.
    """
    reader = pq.ParquetFile(path, pre_buffer=False, buffer_size=FILE_BUFFER_BYTES)
    bounds = iter(days.items())
    day, left = "", 0
    for batch in reader.iter_batches(batch_size=ROLLUP_BATCH_ROWS):
        while batch.num_rows:
            while not left:
                day, left = next(bounds, ("", -1))
            if left < 0:
                raise RuntimeError(f"{path} holds more rows than it records")
            piece = batch.slice(0, left)
            yield day, piece
            left -= piece.num_rows
            batch = batch.slice(piece.num_rows)


def _batch_digests(batch: pa.RecordBatch) -> list[bytes]:
    """Each row's identity; only payload, the last field, may hold a NUL byte."""
    columns = (batch.column(name).to_pylist() for name in ROW_FIELDS)
    return [
        hashlib.blake2b("\0".join(row).encode(), digest_size=16).digest()
        for row in zip(*columns, strict=True)
    ]


def _redelivered(month: str, day: str) -> bool:
    """Whether the day of the month is one with copies to drop."""
    return REDELIVERY_DAYS[0] <= f"{month}/{day}" <= REDELIVERY_DAYS[1]


def _first_rows(
    batches: Iterator[tuple[str, pa.RecordBatch]], month: str
) -> Iterator[tuple[str, pa.RecordBatch]]:
    """The batches without rows identical to one already seen on the same or the previous day.

    Only the redelivery days lose rows. A message and its copies are written within a day of
    each other, so two days of rows bound the memory; a repeat further apart would stay.
    """
    previous: set[bytes] = set()
    current: set[bytes] = set()
    current_day = None
    for day, batch in batches:
        if day != current_day:
            previous, current, current_day = current, set(), day
        if not _redelivered(month, day):
            yield day, batch
            continue
        keep = []
        for digest in _batch_digests(batch):
            first = digest not in current and digest not in previous
            if first:
                current.add(digest)
            keep.append(first)
        yield day, batch.filter(pa.array(keep, pa.bool_()))


def _write_deduped(
    client: S3Client, source: str, month: str, days: dict[str, int], key: str
) -> dict[str, int]:
    """Upload the local monthly file without repeats to `key`; return each day's rows.

    The columns come out in the order the archive writes today.
    """
    source_schema = pq.read_schema(source)
    schema = pa.schema([source_schema.field(name) for name in ARCHIVE_COLUMNS])
    kept = dict.fromkeys(days, 0)
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        with pq.ParquetWriter(tmp.name, schema, compression="zstd") as writer:
            for day, batch in _first_rows(_day_batches(source, days), month):
                if batch.num_rows:
                    writer.write_batch(_conform(batch, schema))
                    kept[day] += batch.num_rows
            writer.add_key_value_metadata({DAILY_ROWS: json.dumps(kept)})
        client.upload_file(tmp.name, BUCKET, key)
    return kept


def _verify_deduped(
    source: str, written: str, month: str, days: dict[str, int], kept: dict[str, int]
) -> None:
    """Raise unless the written file holds the source's rows in order, less only repeats.

    A row may be left out only on a redelivery day and only beside an identical row kept on
    the same or the previous day.
    """
    footer = pq.read_metadata(written)
    if _recorded_days(footer) != kept or footer.num_rows != sum(kept.values()):
        raise RuntimeError(f"rewrite holds {footer.num_rows} rows, recorded {kept}")
    names = footer.schema.to_arrow_schema().names
    if tuple(names) != ARCHIVE_COLUMNS:
        raise RuntimeError(f"rewrite has columns {names}")
    for day, rows in days.items():
        if not _redelivered(month, day) and kept[day] != rows:
            raise RuntimeError(f"day {day} has {kept[day]} rows, the original {rows}")

    output = (
        (day, digest)
        for day, batch in _day_batches(written, kept)
        for digest in _batch_digests(batch)
    )
    ahead = next(output, None)
    previous: set[bytes] = set()
    current: set[bytes] = set()
    current_day = None
    number = 0
    for day, batch in _day_batches(source, days):
        if day != current_day:
            previous, current, current_day = current, set(), day
        for digest in _batch_digests(batch):
            number += 1
            if ahead == (day, digest):
                current.add(digest)
                ahead = next(output, None)
            elif digest not in current and digest not in previous:
                raise RuntimeError(f"row {number} of the original is missing from the rewrite")
    if ahead is not None:
        raise RuntimeError("the rewrite holds rows the original does not")


def _dedupe(client: S3Client, stream: str, month: str) -> str:
    """Rewrite the month's monthly file without repeats; the original stays beside it."""
    prefix = f"{stream}/{month}/"
    monthly = f"{prefix}monthly.parquet"
    backup = f"{prefix}{MONTHLY_BACKUP}"
    rewrite = f"{prefix}{MONTHLY_REWRITE}"
    if not _exists(client, backup):
        _copy(client, monthly, backup)
    with _downloaded(client, backup) as source:
        days = _recorded_days(pq.read_metadata(source))
        try:
            kept = _write_deduped(client, source, month, days, rewrite)
            with _downloaded(client, rewrite) as written:
                _verify_deduped(source, written, month, days, kept)
        except Exception:
            # An unverified rewrite never replaces the monthly file.
            _delete_keys(client, [rewrite])
            raise
    _copy(client, rewrite, monthly)
    _delete_keys(client, [rewrite])
    log.info(
        "compaction.dedupe",
        stream=stream,
        month=month,
        rows=sum(days.values()),
        kept=sum(kept.values()),
    )
    return "deduped"


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
    dedupe_month = os.environ.get("COMPACT_DEDUPE_MONTH")
    # An explicit day names exactly one target, so backfill and rollup only apply without it.
    backfill = 0 if explicit_day else int(os.environ.get("COMPACT_BACKFILL_DAYS", "0"))
    rollup = 0 if explicit_day else int(os.environ.get("COMPACT_ROLLUP_MONTHS", "0"))
    # A month is rolled up once the month after it is over, i.e. when it is older than last month.
    last_month = _last_month(now)

    # Discovery would include streams whose repeats are not exact, so deduplication names its own.
    if dedupe_month and not os.environ.get("COMPACT_STREAMS", "").strip():
        log.error("compaction.start", error="COMPACT_DEDUPE_MONTH needs COMPACT_STREAMS")
        sys.exit(1)

    client = _build_client()
    streams = _resolve_streams(client)
    log.info(
        "compaction.start",
        day=day,
        backfill_days=backfill,
        rollup_months=rollup,
        dedupe_month=dedupe_month,
        streams=list(streams),
    )

    failed: list[str] = []
    statuses: Counter[str] = Counter()

    def run(job: Callable[[S3Client, str, str], str], stream: str, unit: str, target: str) -> str:
        """Run one day or month and log it; a failure is recorded and reported as "failed"."""
        try:
            status = job(client, stream, target)
        except Exception as exc:
            log.error(
                "compaction.stream",
                stream=stream,
                **{unit: target},
                status="failed",
                error=str(exc),
            )
            failed.append(f"{stream}@{target}")
            return "failed"
        statuses[status] += 1
        log.info("compaction.stream", stream=stream, **{unit: target}, status=status)
        return status

    for stream in streams:
        # Deduplicating a month is the run's only job.
        if dedupe_month:
            run(_dedupe, stream, "month", dedupe_month)
            continue

        try:
            days = _targets(client, stream, day, today, backfill)
            months = _pending_months(client, stream, last_month) if rollup > 0 else []
        except Exception as exc:  # botocore raises ClientError and a family of connection errors
            log.error("compaction.stream", stream=stream, status="list-failed", error=str(exc))
            failed.append(stream)
            continue

        for target in days:
            run(_compact, stream, "day", target)

        # Only months actually rolled up count, so a month that keeps failing blocks no other.
        rolled = 0
        for month in months:
            if rolled == rollup:
                break
            if run(_rollup, stream, "month", month) == "rolled-up":
                rolled += 1

    summary = (
        {"month": dedupe_month, "deduped": statuses["deduped"]}
        if dedupe_month
        else {"day": day, "compacted": statuses["compacted"], "rolled_up": statuses["rolled-up"]}
    )
    if failed:
        log.warning("compaction.done", **summary, failed=failed)
        sys.exit(1)
    log.info("compaction.done", **summary)


if __name__ == "__main__":
    main()
