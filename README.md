# nats-archive-compactor

Daily compactor for the `nats-archive` bucket on RustFS. Merges a day's hourly Parquet files per stream into one `daily.parquet`, then deletes the merged hour files; once a month is over by a month, rolls its daily files up into one `monthly.parquet`. Designed to be run as a Kubernetes CronJob.

## What it does

Streams are the top-level prefixes of the bucket, discovered at each run, so a stream a new sidecar starts writing is compacted without a code change.

For a given UTC date (yesterday by default) and each stream, the compactor:

1. Skips the stream if `nats-archive/<stream>/YYYY/MM/DD/daily.parquet` already exists.
2. Otherwise lists `nats-archive/<stream>/YYYY/MM/DD/HH/*.parquet` and merges them with [pyarrow](https://arrow.apache.org/docs/python/) into one ZSTD-compressed daily file. The files must share one schema; otherwise the day fails and nothing is deleted.
3. Deletes exactly the merged keys, only after the daily file is verified. A file written after the listing is left in place, never deleted unmerged.

With `COMPACT_BACKFILL_DAYS` set, each run also catches up days that never got a daily file, newest first and bounded by that number, skipping the current day because its hours are still being written.

With `COMPACT_ROLLUP_MONTHS` set, each run then rolls up, per stream and oldest first, up to that many months whose following month is over. A month that cannot be rolled up fails the run and the next one is tried, so it blocks no other:

```
<stream>/2026/08/monthly.parquet          rolled up (September is over)
<stream>/2026/09/15/daily.parquet         still daily
<stream>/2026/10/24/09/<uuid>.parquet     still hourly, compacted tonight
```

1. Only daily files and empty marker objects may be in the month; anything else (an hour file never merged) leaves the month alone and fails the run.
2. The daily files, which must share one schema, are streamed in batches of 16,384 rows into `monthly.parquet`, so a day of any size fits in memory; its footer records each day's row count under `daily_rows`. The newest day sets the month's schema, so a month gets the column order the archive writes today; the April 2026 monthly files predate this and have `time, subject, payload`. Only column order and nullability may differ between days: DuckDB wrote the first daily files (1 May 2026) with nullable columns, `warp_charge_tracker`'s 1 May was made by hand with its columns in another order, and a null where the month's schema forbids one fails the month.
3. Nothing is deleted unless the monthly file's row count equals the recorded total and every daily file left has exactly its recorded rows. A freshly written monthly file that fails this check is removed again, so the month stays daily.
4. Then the daily files and the markers beside them are deleted. A run cut off while deleting finishes on the next run, which checks the remaining days against the recorded rows.

With `COMPACT_DEDUPE_MONTH` set, the run does nothing else: for each stream named in `COMPACT_STREAMS` it rewrites that month's `monthly.parquet` with its columns in the order the archive writes today, and from 1 to 27 May 2026 without NATS redelivery copies. On those days NATS redelivered every message every 30 s until its batch was written; where `time` comes from the payload (knx, solaredge), each copy is a row identical in `time`, `subject` and `payload`. Every other day keeps all its rows. The streams stamped with the time a delivery was processed (ems_esp, warp_\*) give each copy its own `time`, so their rows go only where the rules below prove a copy.

1. The monthly file is copied to `monthly.parquet.orig` beside it, unless that key exists; a later run reads from it, so a run cut off is simply repeated. The backup stays until the rewritten month is accepted.
2. The backup is downloaded and streamed into `monthly.parquet.new`, keeping the first of each set of identical rows in file order. Copies land within a day of their original, so only the current and the previous day's rows are remembered. `daily_rows` records the rows kept per day. For the streams stamped at delivery instead, the whole month is classified first, which takes about 2 GiB for 15 million rows, and logged like a dry run; unless every payload of a subject keeps a row on each UTC day it was delivered or the day before, nothing is written. Then the rows not proven a copy are streamed into `monthly.parquet.new`.
3. The new file is downloaded again and walked beside the backup: every row left out needs an identical row kept on the same or the previous day of a redelivery day, every other day must keep all its rows, and the file holds nothing else. For the streams stamped at delivery instead, the file must hold exactly the rows the classification kept, in order, and a row left out must lie in the redelivery window. Only then is it copied over `monthly.parquet`; otherwise it is deleted and the month stays as it was.

With `COMPACT_DEDUPE_DRY_RUN=1` as well, the run writes nothing and logs one `compaction.dedupe.day` line per UTC day and subject: rows, rows kept, and rows dropped per reason. It reads what a rewrite would read, the backup once there is one. For the streams stamped at delivery, a row from 1 May until 27 May 19:52 UTC is a copy only when something proves it; every rule keeps the earliest row of each payload:

- **Same payload:** a subject whose payload carries a counter or clock (`ems-esp.heartbeat`, `warp.rtc.time`, the WARP `low_level_state` and `state`, `warp.ntp.state`, `warp.meters.0.errors`) never sends a payload twice, so a repeat is a copy.
- **Beyond the device's count:** ems-esp sends each of its six periodic subjects once per 10 s. JetStream first-delivers in publishing order, so between two heartbeats arrive at most as many rows per subject as their uptimes differ by in tens of seconds; the rest are copies. The earliest row of each payload per day always stays.
- **After 30 s in a batch still collecting:** any other subject's row is a copy when an identical delivery came 30 s before and its batch was certainly still collecting then. That shows where the file's batches are known for certain, or where a counter subject in the same batch was itself redelivered around the row.

All S3 access goes through boto3 on plain keys. pyarrow's S3 filesystem emulates directories and writes an empty marker object for the parent after every delete, so it is only used for the Parquet encoding. boto3 retries transient errors itself (standard mode, 3 attempts in total, and a download cut off mid-body) so a slow response from the storage backend does not abort the stream. Failures on individual streams are logged and the process exits non-zero only if any stream failed, so Kubernetes' `backoffLimit` semantics still work as expected.

## Configuration

| Env var               | Description                                                                                     |
| --------------------- | ----------------------------------------------------------------------------------------------- |
| `RUSTFS_URL`          | Required. e.g. `http://rustfs-svc.rustfs.svc.cluster.local:9000`                                |
| `ACCESS_KEY`          | Required. RustFS S3 access key                                                                  |
| `SECRET_KEY`          | Required. RustFS S3 secret key                                                                  |
| `COMPACT_DAY`         | Optional. `YYYY/MM/DD` override; defaults to yesterday in UTC. Names one day, so it disables backfill and rollup |
| `COMPACT_STREAMS`     | Optional. Comma-separated subset of streams (e.g. `knx,ems_esp`); defaults to every prefix in the bucket |
| `COMPACT_BACKFILL_DAYS` | Optional. Per stream, how many days without a daily file to catch up per run (default `0`, i.e. one day only) |
| `COMPACT_ROLLUP_MONTHS` | Optional. Per stream, how many finished months to roll up into a monthly file per run (default `0`, off) |
| `COMPACT_DEDUPE_MONTH` | Optional. `YYYY/MM` whose monthly file loses its redelivered copies; needs `COMPACT_STREAMS` and disables everything else |
| `COMPACT_DEDUPE_DRY_RUN` | Optional. `1` only counts per day and subject what the month would lose and writes nothing (default `0`) |
| `S3_REQUEST_TIMEOUT`  | Optional. Read timeout per S3 call in seconds (default `60`)                                    |
| `S3_CONNECT_TIMEOUT`  | Optional. Connect timeout in seconds (default `10`)                                             |

## Run locally

```sh
uv venv
uv pip install -e .[dev]
RUSTFS_URL=http://localhost:9000 ACCESS_KEY=... SECRET_KEY=... uv run nats-archive-compactor
```

The tests run against a throwaway `rustfs/rustfs` container, so they need Docker:

```sh
uv run pytest
```

## License

GPL-2.0-or-later. See `LICENSE`.
