"""Example HTML source: Hacker News front page via Playwright."""

from __future__ import annotations

from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import TYPE_CHECKING, Any

import structlog

from altdata.core.base_source import BaseSource, FetchResult

if TYPE_CHECKING:
    from altdata.core.http_client import HttpClient

logger = structlog.get_logger(__name__)

_HN_URL = "https://news.ycombinator.com"


class _HNParser(HTMLParser):
    """Minimal HTML parser for Hacker News story rows.

    Extracts story titles, links, and points from the front page HTML.
    Works against the standard HN DOM structure without external dependencies.
    """

    def __init__(self) -> None:
        super().__init__()
        self.stories: list[dict[str, Any]] = []
        self._in_title_span = False
        self._in_score_span = False
        self._current_story: dict[str, Any] = {}
        self._current_attrs: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_dict = {k: (v or "") for k, v in attrs}

        if tag == "span" and "titleline" in attr_dict.get("class", ""):
            self._in_title_span = True
            self._current_story = {}

        if self._in_title_span and tag == "a":
            href = attr_dict.get("href", "")
            # Resolve relative HN links
            if href.startswith("item?"):
                href = f"{_HN_URL}/{href}"
            self._current_story["url"] = href

        if tag == "span" and "score" in attr_dict.get("class", ""):
            self._in_score_span = True
            self._current_story.setdefault("points", 0)

    def handle_endtag(self, tag: str) -> None:
        if tag == "span" and self._in_title_span:
            self._in_title_span = False
            if self._current_story.get("url") and self._current_story.get("title"):
                self.stories.append(dict(self._current_story))
                self._current_story = {}
        if tag == "span" and self._in_score_span:
            self._in_score_span = False

    def handle_data(self, data: str) -> None:
        if self._in_title_span and "url" in self._current_story and "title" not in self._current_story:
            text = data.strip()
            if text:
                self._current_story["title"] = text
        if self._in_score_span:
            try:
                self._current_story["points"] = int(data.split()[0])
            except (ValueError, IndexError):
                pass


class ExampleHTMLSource(BaseSource):
    """Scrape the Hacker News front page using Playwright for JS rendering.

    Fetches the front page every 30 minutes and extracts story titles,
    links, and upvote points.

    Class Attributes:
        source_id: ``"example_hn_frontpage"``
        schedule: Every 30 minutes.
        use_proxy: False.
        use_playwright: True.
    """

    source_id = "example_hn_frontpage"
    schedule = "*/30 * * * *"
    use_proxy = False
    use_playwright = True

    async def fetch(self, client: HttpClient) -> list[FetchResult]:
        """Fetch the HN front page via Playwright headless browser.

        Args:
            client: Configured HttpClient (Playwright path used).

        Returns:
            A single-element list with the rendered HTML.
        """
        log = logger.bind(source_id=self.source_id, url=_HN_URL)
        html_content, status_code = await client.get_playwright(_HN_URL)
        log.info("html_fetched", status=status_code, bytes=len(html_content))

        return [
            FetchResult(
                source_id=self.source_id,
                run_id="",  # filled in by JobRunner
                fetched_at=datetime.now(tz=timezone.utc),
                url=_HN_URL,
                status_code=status_code,
                raw_payload=html_content.encode("utf-8"),
                content_type="text/html; charset=utf-8",
                metadata={"page": "frontpage"},
            )
        ]

    def parse(self, result: FetchResult) -> list[dict[str, Any]]:
        """Parse rendered HN HTML into a list of story records.

        Args:
            result: FetchResult containing rendered HTML bytes.

        Returns:
            List of dicts with keys: url, title, body, points, fetched_at.
        """
        html = result.raw_payload.decode("utf-8", errors="replace")
        parser = _HNParser()
        parser.feed(html)

        records: list[dict[str, Any]] = []
        for story in parser.stories:
            records.append(
                {
                    "url": story.get("url", ""),
                    "title": story.get("title", ""),
                    "body": None,
                    "fetched_at": result.fetched_at,
                    "published_at": None,
                    "points": story.get("points", 0),
                    "source": self.source_id,
                }
            )

        logger.info("html_parsed", source_id=self.source_id, record_count=len(records))
        return records

    def source_key(self, record: dict[str, Any]) -> str:
        """Use the story URL as the stable dedup key.

        Args:
            record: Normalised record dict.

        Returns:
            The story URL string.
        """
        return record.get("url", "")
