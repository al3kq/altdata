"""Async HTTP client with retry logic, proxy support, and optional Playwright rendering."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

import httpx
import structlog

if TYPE_CHECKING:
    from altdata.core.proxy import ProxyProvider
    from altdata.settings import Settings

logger = structlog.get_logger(__name__)

# HTTP status codes that should trigger a retry
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class MaxRetriesExceeded(Exception):
    """Raised when all retry attempts for an HTTP request have been exhausted.

    Attributes:
        url: The URL that was being fetched.
        attempts: Number of attempts made.
        last_exception: The last exception encountered.
    """

    def __init__(self, url: str, attempts: int, last_exception: Exception) -> None:
        self.url = url
        self.attempts = attempts
        self.last_exception = last_exception
        super().__init__(
            f"Max retries ({attempts}) exceeded for {url}: {last_exception}"
        )


class HttpClient:
    """Async HTTP client wrapping httpx.AsyncClient with retry and proxy support.

    Provides exponential-backoff retry logic for transient HTTP errors,
    optional proxy routing via a ProxyProvider, and an optional Playwright
    path for JavaScript-heavy pages.

    Args:
        settings: Application settings controlling timeout and retry behaviour.
        proxy_provider: Optional proxy provider; uses no proxy when omitted.
    """

    def __init__(
        self,
        settings: Settings,
        proxy_provider: ProxyProvider | None = None,
    ) -> None:
        self._settings = settings
        self._proxy_provider = proxy_provider
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> HttpClient:
        proxies = self._proxy_provider.httpx_proxies() if self._proxy_provider else {}
        # httpx >= 0.28 removed proxies= in favour of per-scheme mounts.
        if proxies:
            mounts = {
                scheme: httpx.AsyncHTTPTransport(proxy=url)
                for scheme, url in proxies.items()
            }
            self._client = httpx.AsyncClient(
                timeout=self._settings.http_timeout,
                follow_redirects=True,
                mounts=mounts,
            )
        else:
            self._client = httpx.AsyncClient(
                timeout=self._settings.http_timeout,
                follow_redirects=True,
            )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get(self, url: str, **kwargs: Any) -> httpx.Response:
        """Perform an HTTP GET request with exponential-backoff retry.

        Retries on RETRYABLE_STATUS_CODES and connection-level errors.
        Each attempt is logged with url, status code, attempt number, and
        elapsed time in milliseconds.

        Args:
            url: The URL to fetch.
            **kwargs: Additional keyword arguments forwarded to httpx.

        Returns:
            The successful httpx.Response.

        Raises:
            MaxRetriesExceeded: When all retry attempts are exhausted.
            RuntimeError: When called outside an async context manager.
        """
        if self._client is None:
            raise RuntimeError("HttpClient must be used as an async context manager")

        last_exc: Exception = RuntimeError("No attempts made")
        max_retries = self._settings.http_max_retries
        backoff_factor = self._settings.http_backoff_factor

        for attempt in range(max_retries + 1):
            start = time.monotonic()
            try:
                response = await self._client.get(url, **kwargs)
                elapsed_ms = int((time.monotonic() - start) * 1000)

                log = logger.bind(url=url, status=response.status_code, attempt=attempt, elapsed_ms=elapsed_ms)

                if response.status_code in RETRYABLE_STATUS_CODES:
                    last_exc = httpx.HTTPStatusError(
                        f"Retryable status {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                    log.warning("http_retryable_status")
                    if attempt < max_retries:
                        delay = backoff_factor ** attempt
                        await asyncio.sleep(delay)
                    continue

                log.info("http_get_success")
                return response

            except (httpx.ConnectError, httpx.TimeoutException, httpx.RemoteProtocolError) as exc:
                elapsed_ms = int((time.monotonic() - start) * 1000)
                last_exc = exc
                logger.warning(
                    "http_connection_error",
                    url=url,
                    attempt=attempt,
                    elapsed_ms=elapsed_ms,
                    error=str(exc),
                )
                if attempt < max_retries:
                    delay = backoff_factor ** attempt
                    await asyncio.sleep(delay)

        raise MaxRetriesExceeded(url=url, attempts=max_retries + 1, last_exception=last_exc)

    async def get_playwright(self, url: str) -> tuple[str, int]:
        """Fetch a URL using a headless browser and return rendered HTML.

        Uses Playwright's async API to load the page with JavaScript execution,
        enabling scraping of client-rendered content.

        Args:
            url: The URL to fetch via headless browser.

        Returns:
            A tuple of (html_content, status_code).

        Raises:
            ImportError: When playwright is not installed.
            RuntimeError: On browser launch or navigation failure.
        """
        try:
            from playwright.async_api import async_playwright
        except ImportError as exc:
            raise ImportError(
                "Playwright is not installed. Install it with: "
                "pip install playwright && playwright install chromium"
            ) from exc

        headless = self._settings.playwright_headless
        log = logger.bind(url=url)

        start = time.monotonic()
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=headless)
            try:
                page = await browser.new_page()
                response = await page.goto(url, wait_until="networkidle")
                if response is None:
                    raise RuntimeError(f"Playwright returned no response for {url}")
                status_code = response.status
                html_content = await page.content()
            finally:
                await browser.close()

        elapsed_ms = int((time.monotonic() - start) * 1000)
        log.info("playwright_get_success", status=status_code, elapsed_ms=elapsed_ms)
        return html_content, status_code
