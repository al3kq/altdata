"""Tests for BaseSource and FetchResult dataclass."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import pytest

from altdata.core.base_source import BaseSource, FetchResult


# ---------------------------------------------------------------------------
# Concrete stub for testing abstract class behaviour
# ---------------------------------------------------------------------------

class StubSource(BaseSource):
    source_id = "stub_source"
    schedule = "0 * * * *"
    use_proxy = False
    use_playwright = False

    async def fetch(self, client: Any) -> list[FetchResult]:
        return []

    def parse(self, result: FetchResult) -> list[dict[str, Any]]:
        return [{"url": "https://example.com", "title": "Test"}]


class CustomKeySource(StubSource):
    source_id = "custom_key_source"

    def source_key(self, record: dict[str, Any]) -> str:
        return record.get("title", "")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_fetch_result_defaults() -> None:
    """FetchResult can be instantiated with minimal arguments."""
    result = FetchResult(
        source_id="src",
        run_id=str(uuid.uuid4()),
        fetched_at=datetime.now(tz=timezone.utc),
        url="https://example.com",
        status_code=200,
        raw_payload=b"hello",
        content_type="text/html",
    )
    assert result.metadata == {}
    assert result.raw_payload == b"hello"


def test_base_source_defaults() -> None:
    """BaseSource subclass has correct default flags."""
    source = StubSource()
    assert source.use_proxy is False
    assert source.use_playwright is False


def test_default_source_key() -> None:
    """Default source_key() uses the 'url' field."""
    source = StubSource()
    record = {"url": "https://example.com/article", "title": "Article"}
    assert source.source_key(record) == "https://example.com/article"


def test_custom_source_key() -> None:
    """Overriding source_key() uses custom logic."""
    source = CustomKeySource()
    record = {"url": "https://example.com/article", "title": "My Title"}
    assert source.source_key(record) == "My Title"


def test_source_key_missing_url() -> None:
    """source_key() returns empty string when url is absent."""
    source = StubSource()
    assert source.source_key({}) == ""


def test_cannot_instantiate_base_source() -> None:
    """BaseSource cannot be instantiated directly (abstract)."""
    with pytest.raises(TypeError):
        BaseSource()  # type: ignore[abstract]


@pytest.mark.asyncio
async def test_stub_source_fetch_returns_empty() -> None:
    """Stub fetch() returns an empty list."""
    source = StubSource()
    result = await source.fetch(None)  # type: ignore[arg-type]
    assert result == []


def test_stub_source_parse_returns_records() -> None:
    """Stub parse() returns a list of dicts."""
    source = StubSource()
    result = FetchResult(
        source_id="stub_source",
        run_id="r1",
        fetched_at=datetime.now(tz=timezone.utc),
        url="https://example.com",
        status_code=200,
        raw_payload=b"<html/>",
        content_type="text/html",
    )
    records = source.parse(result)
    assert isinstance(records, list)
    assert len(records) == 1
    assert records[0]["url"] == "https://example.com"
