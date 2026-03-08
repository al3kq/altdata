"""Shared pytest fixtures for the altdata test suite."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator

import pytest
import pytest_asyncio
import respx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from altdata.core.base_source import FetchResult
from altdata.core.raw_store import DiskRawStore
from altdata.db.models import Base
from altdata.settings import Settings

# ---------------------------------------------------------------------------
# Settings fixture — SQLite in-memory for all tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def test_settings(tmp_path_factory: pytest.TempPathFactory) -> Settings:
    """Settings instance wired to SQLite in-memory and a temporary raw store."""
    raw_path = tmp_path_factory.mktemp("raw_store")
    return Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        raw_store_backend="disk",
        raw_store_path=raw_path,
        oxylabs_username="",
        oxylabs_password="",
        http_timeout=5.0,
        http_max_retries=2,
        http_backoff_factor=0.01,  # fast for tests
    )


# ---------------------------------------------------------------------------
# Async DB session fixture
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session")
async def db_engine(test_settings: Settings):
    """Create a shared SQLite in-memory engine with all tables."""
    engine = create_async_engine(test_settings.database_url, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def async_session(db_engine) -> AsyncGenerator[AsyncSession, None]:
    """Yield a fresh session for each test, rolling back on completion."""
    factory = async_sessionmaker(bind=db_engine, expire_on_commit=False, autoflush=False)
    async with factory() as session:
        yield session
        await session.rollback()


# ---------------------------------------------------------------------------
# RawStore fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_raw_store(tmp_path: Path) -> DiskRawStore:
    """DiskRawStore pointing at a temporary directory."""
    return DiskRawStore(tmp_path / "raw")


# ---------------------------------------------------------------------------
# FetchResult fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_fetch_result() -> FetchResult:
    """A FetchResult with deterministic dummy data for testing."""
    return FetchResult(
        source_id="test_source",
        run_id=str(uuid.uuid4()),
        fetched_at=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
        url="https://example.com/feed",
        status_code=200,
        raw_payload=b"<rss><channel><title>Test</title></channel></rss>",
        content_type="application/rss+xml",
        metadata={"feed": "test"},
    )


# ---------------------------------------------------------------------------
# respx mock fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_httpx():
    """Activate a respx mock router for httpx calls."""
    with respx.mock(assert_all_called=False) as router:
        yield router
