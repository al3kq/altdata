"""Tests for JobRunner happy path and failure path."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from altdata.core.base_source import BaseSource, FetchResult
from altdata.core.job_runner import JobRunner, RunSummary
from altdata.settings import Settings


# ---------------------------------------------------------------------------
# Stub source
# ---------------------------------------------------------------------------

class _GoodSource(BaseSource):
    source_id = "good_source"
    schedule = "0 * * * *"
    use_proxy = False
    use_playwright = False

    def __init__(self, results: list[FetchResult], records: list[dict[str, Any]]) -> None:
        self._results = results
        self._records = records

    async def fetch(self, client: Any) -> list[FetchResult]:
        return self._results

    def parse(self, result: FetchResult) -> list[dict[str, Any]]:
        return self._records


class _FailingSource(BaseSource):
    source_id = "failing_source"
    schedule = "0 * * * *"
    use_proxy = False
    use_playwright = False

    async def fetch(self, client: Any) -> list[FetchResult]:
        raise RuntimeError("Network failure!")

    def parse(self, result: FetchResult) -> list[dict[str, Any]]:
        return []


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_result() -> FetchResult:
    return FetchResult(
        source_id="good_source",
        run_id="",
        fetched_at=datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc),
        url="https://example.com/article",
        status_code=200,
        raw_payload=b"<html>content</html>",
        content_type="text/html",
    )


@pytest.fixture
def sample_record() -> dict[str, Any]:
    return {
        "url": "https://example.com/article",
        "title": "Test Article",
        "body": "Article body",
        "fetched_at": datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc),
    }


@pytest.fixture
def runner_settings(tmp_path: Path) -> Settings:
    return Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        raw_store_backend="disk",
        raw_store_path=tmp_path / "raw",
        http_timeout=5.0,
        http_max_retries=1,
        http_backoff_factor=0.01,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_happy_path(
    runner_settings: Settings,
    sample_result: FetchResult,
    sample_record: dict[str, Any],
    tmp_path: Path,
) -> None:
    """JobRunner returns RunSummary with status='success' on a clean run."""
    source = _GoodSource(results=[sample_result], records=[sample_record])

    with (
        patch("altdata.core.job_runner.get_session_factory") as mock_factory,
        patch("altdata.core.job_runner.get_raw_store") as mock_store_factory,
    ):
        # Mock session factory
        mock_run = MagicMock()
        mock_run.id = uuid.uuid4()

        mock_run_repo = AsyncMock()
        mock_run_repo.create_run.return_value = mock_run
        mock_run_repo.complete_run.return_value = mock_run

        mock_payload_repo = AsyncMock()
        mock_payload_repo.upsert_payload.return_value = (MagicMock(), True)

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_factory.return_value = MagicMock(return_value=mock_session)

        # Mock raw store
        mock_store = AsyncMock()
        mock_store.save.return_value = tmp_path / "raw" / "test_source" / "2024-06-01" / "abc.bin"
        mock_store_factory.return_value = mock_store

        with patch("altdata.core.job_runner.RunRepo", return_value=mock_run_repo):
            with patch("altdata.core.job_runner.PayloadRepo", return_value=mock_payload_repo):
                runner = JobRunner(runner_settings)
                summary = await runner.run(source)

    assert summary.status == "success"
    assert summary.source_id == "good_source"
    assert summary.records_fetched == 1


@pytest.mark.asyncio
async def test_failure_path(
    runner_settings: Settings,
    tmp_path: Path,
) -> None:
    """JobRunner returns status='failed' when fetch() raises an exception."""
    source = _FailingSource()

    with (
        patch("altdata.core.job_runner.get_session_factory") as mock_factory,
        patch("altdata.core.job_runner.get_raw_store") as mock_store_factory,
    ):
        mock_run = MagicMock()
        mock_run.id = uuid.uuid4()

        mock_run_repo = AsyncMock()
        mock_run_repo.create_run.return_value = mock_run
        mock_run_repo.fail_run.return_value = mock_run

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_factory.return_value = MagicMock(return_value=mock_session)

        mock_store = AsyncMock()
        mock_store_factory.return_value = mock_store

        with patch("altdata.core.job_runner.RunRepo", return_value=mock_run_repo):
            with patch("altdata.core.job_runner.PayloadRepo"):
                runner = JobRunner(runner_settings)
                summary = await runner.run(source)

    assert summary.status == "failed"
    assert summary.error_message is not None
    assert "Network failure" in summary.error_message


def test_run_summary_defaults() -> None:
    """RunSummary can be constructed with minimal arguments."""
    summary = RunSummary(run_id="abc", source_id="src", status="success")
    assert summary.records_fetched == 0
    assert summary.records_upserted == 0
    assert summary.raw_store_paths == []
    assert summary.error_message is None
