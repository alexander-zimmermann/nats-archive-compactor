# nats-archive-compactor

Daily compactor for the `nats-archive` bucket on RustFS. Merges 24 hourly Parquet files per stream into one `daily.parquet`, then deletes the hour-folders. Designed to be run as a Kubernetes CronJob.

## What it does

Streams are the top-level prefixes of the bucket, discovered at each run, so a stream a new sidecar starts writing is compacted without a code change.

For a given UTC date (yesterday by default) and each stream, the compactor:

1. Skips the stream if `nats-archive/<stream>/YYYY/MM/DD/daily.parquet` already exists.
2. Otherwise lists `nats-archive/<stream>/YYYY/MM/DD/HH/*.parquet` hour-by-hour and merges them with [pyarrow](https://arrow.apache.org/docs/python/) into one ZSTD-compressed daily file.
3. Removes the source hour-folders only after the daily file is verified.

With `COMPACT_BACKFILL_DAYS` set, each run also catches up days that never got a daily file, newest first and bounded by that number, skipping the current day because its hours are still being written.

S3 list/read/write calls are wrapped with exponential-backoff retries (`tenacity`) so a transient slow response from the storage backend does not abort the stream. Failures on individual streams are logged and the process exits non-zero only if any stream failed, so Kubernetes' `backoffLimit` semantics still work as expected.

## Configuration

| Env var               | Description                                                                                     |
| --------------------- | ----------------------------------------------------------------------------------------------- |
| `RUSTFS_URL`          | Required. e.g. `http://rustfs-svc.rustfs.svc.cluster.local:9000`                                |
| `ACCESS_KEY`          | Required. RustFS S3 access key                                                                  |
| `SECRET_KEY`          | Required. RustFS S3 secret key                                                                  |
| `COMPACT_DAY`         | Optional. `YYYY/MM/DD` override; defaults to yesterday in UTC. Names one day, so it disables backfill |
| `COMPACT_STREAMS`     | Optional. Comma-separated subset of streams (e.g. `knx,ems_esp`); defaults to every prefix in the bucket |
| `COMPACT_BACKFILL_DAYS` | Optional. Per stream, how many days without a daily file to catch up per run (default `0`, i.e. one day only) |
| `S3_REQUEST_TIMEOUT`  | Optional. Per-request timeout in seconds (default `60`)                                         |
| `S3_CONNECT_TIMEOUT`  | Optional. Connect timeout in seconds (default `10`)                                             |

## Run locally

```sh
uv venv
uv pip install -e .[dev]
RUSTFS_URL=http://localhost:9000 ACCESS_KEY=... SECRET_KEY=... uv run nats-archive-compactor
```

## License

GPL-2.0-or-later. See `LICENSE`.
