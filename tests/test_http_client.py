"""Tests for HttpClient retry logic and backoff behaviour."""

from __future__ import annotations

import httpx
import pytest
import respx

from altdata.core.http_client import HttpClient, MaxRetriesExceeded
from altdata.core.proxy import NullProxyProvider
from altdata.settings import Settings


@pytest.fixture
def fast_settings() -> Settings:
    return Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        http_timeout=5.0,
        http_max_retries=2,
        http_backoff_factor=0.01,  # near-zero backoff for speed
    )


@pytest.mark.asyncio
async def test_successful_get(fast_settings: Settings) -> None:
    """A 200 response is returned immediately without retries."""
    with respx.mock() as router:
        route = router.get("https://example.com/ok").mock(return_value=httpx.Response(200, text="OK"))

        async with HttpClient(fast_settings, NullProxyProvider()) as client:
            response = await client.get("https://example.com/ok")

        assert response.status_code == 200
        assert route.call_count == 1


@pytest.mark.asyncio
async def test_retry_on_429(fast_settings: Settings) -> None:
    """Client retries on 429 and succeeds on the third attempt."""
    with respx.mock() as router:
        route = router.get("https://example.com/rate-limited").mock(
            side_effect=[
                httpx.Response(429),
                httpx.Response(429),
                httpx.Response(200, text="OK"),
            ]
        )

        async with HttpClient(fast_settings, NullProxyProvider()) as client:
            response = await client.get("https://example.com/rate-limited")

        assert response.status_code == 200
        assert route.call_count == 3


@pytest.mark.asyncio
async def test_retry_on_503(fast_settings: Settings) -> None:
    """Client retries on 503."""
    with respx.mock() as router:
        router.get("https://example.com/server-error").mock(
            side_effect=[
                httpx.Response(503),
                httpx.Response(200, text="recovered"),
            ]
        )

        async with HttpClient(fast_settings, NullProxyProvider()) as client:
            response = await client.get("https://example.com/server-error")

        assert response.status_code == 200


@pytest.mark.asyncio
async def test_max_retries_exceeded(fast_settings: Settings) -> None:
    """MaxRetriesExceeded is raised after all retry attempts fail."""
    with respx.mock() as router:
        route = router.get("https://example.com/always-fails").mock(
            side_effect=[httpx.Response(500)] * 3
        )

        async with HttpClient(fast_settings, NullProxyProvider()) as client:
            with pytest.raises(MaxRetriesExceeded) as exc_info:
                await client.get("https://example.com/always-fails")

        assert "always-fails" in str(exc_info.value)
        assert exc_info.value.attempts == 3
        assert route.call_count == 3


@pytest.mark.asyncio
async def test_retry_on_connection_error(fast_settings: Settings) -> None:
    """Connection errors trigger retries."""
    with respx.mock() as router:
        router.get("https://example.com/flaky").mock(
            side_effect=[
                httpx.ConnectError("connection refused"),
                httpx.Response(200, text="OK"),
            ]
        )

        async with HttpClient(fast_settings, NullProxyProvider()) as client:
            response = await client.get("https://example.com/flaky")

        assert response.status_code == 200


@pytest.mark.asyncio
async def test_context_manager_required() -> None:
    """Calling get() outside a context manager raises RuntimeError."""
    settings = Settings(database_url="sqlite+aiosqlite:///:memory:")
    client = HttpClient(settings)
    with pytest.raises(RuntimeError, match="context manager"):
        await client.get("https://example.com/")


@pytest.mark.asyncio
async def test_non_retryable_status_not_retried(fast_settings: Settings) -> None:
    """A 404 is returned immediately without any retries."""
    with respx.mock() as router:
        route = router.get("https://example.com/not-found").mock(return_value=httpx.Response(404))

        async with HttpClient(fast_settings, NullProxyProvider()) as client:
            response = await client.get("https://example.com/not-found")

        assert response.status_code == 404
        assert route.call_count == 1
