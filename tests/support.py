"""Seeding and inspecting the test bucket, with the same client the compactor uses."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from nats_archive_compactor.__main__ import BUCKET, _list_objects

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


@dataclass
class Store:
    client: S3Client

    def put(self, key: str, body: bytes = b"x") -> None:
        self.client.put_object(Bucket=BUCKET, Key=key, Body=body)

    def put_table(self, key: str, table: pa.Table) -> None:
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        self.put(key, sink.getvalue().to_pybytes())

    def put_parquet(self, key: str, rows: int) -> None:
        """An archive-shaped file: one row per message, its subject unique per key."""
        start = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
        self.put_table(
            key,
            pa.table(
                {
                    "time": [start + dt.timedelta(seconds=i) for i in range(rows)],
                    "subject": [f"{key}.{i}" for i in range(rows)],
                    "payload": [f'{{"value": {i}}}' for i in range(rows)],
                }
            ),
        )

    def keys(self) -> list[str]:
        """Every key in the bucket, markers included."""
        return sorted(_list_objects(self.client, ""))

    def table(self, key: str) -> pa.Table:
        body = self.client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        return pq.read_table(pa.BufferReader(body))
