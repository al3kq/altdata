"""Abstract base class and data types for all altdata sources."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from altdata.core.http_client import HttpClient


@dataclass
class FetchResult:
    """Raw result returned by a source's fetch() method.

    Carries the unprocessed HTTP response alongside metadata required
    for storage and tracing.

    Attributes:
        source_id: Unique slug identifying the source.
        run_id: UUID string for the enclosing ScraperRun.
        fetched_at: UTC timestamp when the request completed.
        url: The fully-qualified URL that was fetched.
        status_code: HTTP status code of the response.
        raw_payload: Raw bytes of the response body.
        content_type: Value of the Content-Type response header.
        metadata: Source-specific key/value pairs (e.g. feed name, page index).
    """

    source_id: str
    run_id: str
    fetched_at: datetime
    url: str
    status_code: int
    raw_payload: bytes
    content_type: str
    metadata: dict[str, Any] = field(default_factory=dict)


class BaseSource(ABC):
    """Abstract base class that every altdata source must implement.

    Subclasses declare their identity via class-level attributes and
    implement two methods: ``fetch`` (network I/O) and ``parse``
    (normalisation of raw bytes into structured dicts).

    Class Attributes:
        source_id: Unique slug used as a stable identifier (e.g. "yahoo_finance_rss").
        schedule: Cron expression or APScheduler interval string (e.g. "0 * * * *").
        use_proxy: Whether to route requests through the configured proxy provider.
        use_playwright: Whether to use a headless browser for JavaScript rendering.
    """

    source_id: str
    schedule: str
    use_proxy: bool = False
    use_playwright: bool = False

    @abstractmethod
    async def fetch(self, client: HttpClient) -> list[FetchResult]:
        """Perform HTTP request(s) and return raw results.

        Args:
            client: Configured HttpClient instance (with or without proxy).

        Returns:
            A list of FetchResult objects, one per HTTP request made.
        """

    @abstractmethod
    def parse(self, result: FetchResult) -> list[dict[str, Any]]:
        """Normalise a raw FetchResult into a list of structured records.

        Each returned dict should contain at minimum the fields expected by
        ``payload_repo.upsert_payload``.  Keys should be consistent across
        runs so that idempotent upserts work correctly.

        Args:
            result: A single FetchResult as returned by ``fetch()``.

        Returns:
            A list of normalised dicts ready for database upsert.
        """

    def source_key(self, record: dict[str, Any]) -> str:
        """Return a stable deduplication key for a parsed record.

        The default implementation uses the record's ``url`` field.
        Override this in subclasses that need a different dedup strategy
        (e.g. an article ID embedded in the URL).

        Args:
            record: A single normalised dict as returned by ``parse()``.

        Returns:
            A string that uniquely identifies this record within the source.
        """
        return record.get("url", "")
