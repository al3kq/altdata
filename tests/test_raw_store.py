"""Tests for DiskRawStore save/load roundtrip."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from altdata.core.base_source import FetchResult
from altdata.core.raw_store import DiskRawStore, S3RawStore, get_raw_store
from altdata.settings import Settings


@pytest.mark.asyncio
async def test_save_creates_files(tmp_raw_store: DiskRawStore, sample_fetch_result: FetchResult) -> None:
    """save() writes both a .bin and a .meta.json sidecar."""
    path = await tmp_raw_store.save(sample_fetch_result)

    assert path.exists()
    assert path.suffix == ".bin"
    meta_path = path.with_suffix(".meta.json")
    assert meta_path.exists()


@pytest.mark.asyncio
async def test_save_load_roundtrip(tmp_raw_store: DiskRawStore, sample_fetch_result: FetchResult) -> None:
    """load() reconstructs a FetchResult equivalent to the original."""
    path = await tmp_raw_store.save(sample_fetch_result)
    loaded = await tmp_raw_store.load(path)

    assert loaded.source_id == sample_fetch_result.source_id
    assert loaded.run_id == sample_fetch_result.run_id
    assert loaded.url == sample_fetch_result.url
    assert loaded.status_code == sample_fetch_result.status_code
    assert loaded.raw_payload == sample_fetch_result.raw_payload
    assert loaded.content_type == sample_fetch_result.content_type
    assert loaded.metadata == sample_fetch_result.metadata


@pytest.mark.asyncio
async def test_path_layout(tmp_path: Path, sample_fetch_result: FetchResult) -> None:
    """Payloads are stored under source_id/YYYY-MM-DD/."""
    store = DiskRawStore(tmp_path)
    path = await store.save(sample_fetch_result)

    parts = path.relative_to(tmp_path).parts
    assert parts[0] == sample_fetch_result.source_id
    assert parts[1] == sample_fetch_result.fetched_at.strftime("%Y-%m-%d")


@pytest.mark.asyncio
async def test_different_urls_different_paths(tmp_path: Path) -> None:
    """Two FetchResults with different URLs produce different storage paths."""
    store = DiskRawStore(tmp_path)
    run_id = str(uuid.uuid4())
    now = datetime.now(tz=timezone.utc)

    result_a = FetchResult(
        source_id="src",
        run_id=run_id,
        fetched_at=now,
        url="https://example.com/a",
        status_code=200,
        raw_payload=b"payload_a",
        content_type="text/plain",
    )
    result_b = FetchResult(
        source_id="src",
        run_id=run_id,
        fetched_at=now,
        url="https://example.com/b",
        status_code=200,
        raw_payload=b"payload_b",
        content_type="text/plain",
    )

    path_a = await store.save(result_a)
    path_b = await store.save(result_b)
    assert path_a != path_b


@pytest.mark.asyncio
async def test_s3_raw_store_raises() -> None:
    """S3RawStore raises NotImplementedError for save and load."""
    store = S3RawStore(bucket="my-bucket", prefix="raw/")
    from altdata.core.base_source import FetchResult
    result = FetchResult(
        source_id="s",
        run_id="r",
        fetched_at=datetime.now(tz=timezone.utc),
        url="https://example.com",
        status_code=200,
        raw_payload=b"",
        content_type="text/plain",
    )
    with pytest.raises(NotImplementedError):
        await store.save(result)
    with pytest.raises(NotImplementedError):
        await store.load(Path("/fake/path.bin"))


def test_get_raw_store_disk(tmp_path: Path) -> None:
    """Factory returns DiskRawStore for backend='disk'."""
    settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        raw_store_backend="disk",
        raw_store_path=tmp_path,
    )
    store = get_raw_store(settings)
    assert isinstance(store, DiskRawStore)


def test_get_raw_store_unknown_backend() -> None:
    """Factory raises ValueError for an unrecognised backend."""
    settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        raw_store_backend="disk",  # valid to construct, then override
    )
    settings.raw_store_backend = "ftp"  # type: ignore[assignment]
    with pytest.raises(ValueError, match="ftp"):
        get_raw_store(settings)
