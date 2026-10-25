"""A throwaway rustfs container, the version the cluster runs; it touches nothing in the cluster."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy

from nats_archive_compactor.__main__ import BUCKET, _build_client

from .support import Store

RUSTFS_IMAGE = "rustfs/rustfs:1.0.3"
ACCESS_KEY = "compactor-test-access"
SECRET_KEY = "compactor-test-secret-key"


@pytest.fixture(scope="session")
def rustfs_url() -> Iterator[str]:
    container = (
        DockerContainer(RUSTFS_IMAGE)
        .with_env("RUSTFS_ACCESS_KEY", ACCESS_KEY)
        .with_env("RUSTFS_SECRET_KEY", SECRET_KEY)
        .with_exposed_ports(9000)
        .waiting_for(HttpWaitStrategy(9000, "/health").for_status_code(200))
    )
    container.start()
    try:
        yield f"http://{container.get_container_host_ip()}:{container.get_exposed_port(9000)}"
    finally:
        container.stop()


@pytest.fixture
def store(rustfs_url: str, monkeypatch: pytest.MonkeyPatch) -> Store:
    """An empty nats-archive bucket, reached through the compactor's own client."""
    monkeypatch.setenv("RUSTFS_URL", rustfs_url)
    monkeypatch.setenv("ACCESS_KEY", ACCESS_KEY)
    monkeypatch.setenv("SECRET_KEY", SECRET_KEY)
    monkeypatch.delenv("COMPACT_STREAMS", raising=False)
    store = Store(client=_build_client())
    if BUCKET not in {b["Name"] for b in store.client.list_buckets().get("Buckets", [])}:
        store.client.create_bucket(Bucket=BUCKET)
    for key in store.keys():
        store.client.delete_object(Bucket=BUCKET, Key=key)
    return store
