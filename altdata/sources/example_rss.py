"""Example RSS source: Yahoo Finance news feed."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import structlog

from altdata.core.base_source import BaseSource, FetchResult

if TYPE_CHECKING:
    from altdata.core.http_client import HttpClient

logger = structlog.get_logger(__name__)

_FEED_URL = "https://finance.yahoo.com/news/rssindex"

# RSS namespace map
_NS = {
    "media": "http://search.yahoo.com/mrss/",
    "dc": "http://purl.org/dc/elements/1.1/",
}


class ExampleRSSSource(BaseSource):
    """Scrape the Yahoo Finance RSS news index.

    Fetches the feed every hour and parses each ``<item>`` element into a
    normalised record containing title, link, published date, and description.

    Class Attributes:
        source_id: ``"example_yahoo_finance_rss"``
        schedule: Every hour at minute 0.
        use_proxy: False.
        use_playwright: False.
    """

    source_id = "example_yahoo_finance_rss"
    schedule = "0 * * * *"
    use_proxy = False
    use_playwright = False

    async def fetch(self, client: HttpClient) -> list[FetchResult]:
        """Fetch the Yahoo Finance RSS feed.

        Args:
            client: Configured HttpClient.

        Returns:
            A single-element list containing the raw RSS response.
        """
        log = logger.bind(source_id=self.source_id, url=_FEED_URL)
        response = await client.get(_FEED_URL)
        log.info("rss_fetched", status=response.status_code, bytes=len(response.content))

        return [
            FetchResult(
                source_id=self.source_id,
                run_id="",  # filled in by JobRunner
                fetched_at=datetime.now(tz=timezone.utc),
                url=_FEED_URL,
                status_code=response.status_code,
                raw_payload=response.content,
                content_type=response.headers.get("content-type", "application/rss+xml"),
                metadata={"feed": "yahoo_finance_rss"},
            )
        ]

    def parse(self, result: FetchResult) -> list[dict[str, Any]]:
        """Parse raw RSS XML into a list of normalised article records.

        Args:
            result: FetchResult containing the raw RSS XML bytes.

        Returns:
            List of dicts with keys: url, title, body, published_at, fetched_at.
        """
        records: list[dict[str, Any]] = []
        try:
            root = ET.fromstring(result.raw_payload.decode("utf-8", errors="replace"))
        except ET.ParseError as exc:
            logger.error("rss_parse_error", source_id=self.source_id, error=str(exc))
            return records

        channel = root.find("channel")
        if channel is None:
            logger.warning("rss_no_channel", source_id=self.source_id)
            return records

        for item in channel.findall("item"):
            url = (item.findtext("link") or "").strip()
            title = (item.findtext("title") or "").strip()
            description = (item.findtext("description") or "").strip()
            pub_date_raw = (item.findtext("pubDate") or "").strip()

            published_at: datetime | None = None
            if pub_date_raw:
                try:
                    from dateutil import parser as dateutil_parser

                    published_at = dateutil_parser.parse(pub_date_raw)
                    if published_at.tzinfo is None:
                        published_at = published_at.replace(tzinfo=timezone.utc)
                except (ValueError, OverflowError):
                    published_at = None

            if not url:
                continue

            records.append(
                {
                    "url": url,
                    "title": title,
                    "body": description,
                    "fetched_at": result.fetched_at,
                    "published_at": published_at,
                    "source": self.source_id,
                }
            )

        logger.info("rss_parsed", source_id=self.source_id, record_count=len(records))
        return records

    def source_key(self, record: dict[str, Any]) -> str:
        """Use the article URL as the stable dedup key.

        Args:
            record: Normalised record dict.

        Returns:
            The article URL string.
        """
        return record.get("url", "")
