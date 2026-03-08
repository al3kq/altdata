"""Tests for example sources using fixture data (no real HTTP calls)."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from altdata.core.base_source import FetchResult
from altdata.sources.example_html import ExampleHTMLSource
from altdata.sources.example_rss import ExampleRSSSource

# ---------------------------------------------------------------------------
# RSS source tests
# ---------------------------------------------------------------------------

_RSS_FIXTURE = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Yahoo Finance</title>
    <item>
      <title>Apple Reports Record Earnings</title>
      <link>https://finance.yahoo.com/news/apple-earnings-12345.html</link>
      <pubDate>Sat, 01 Jun 2024 10:00:00 GMT</pubDate>
      <description>Apple Inc. reported record-breaking Q2 earnings on Friday.</description>
    </item>
    <item>
      <title>Fed Holds Rates Steady</title>
      <link>https://finance.yahoo.com/news/fed-rates-67890.html</link>
      <pubDate>Sat, 01 Jun 2024 12:30:00 GMT</pubDate>
      <description>The Federal Reserve opted to hold rates steady at its June meeting.</description>
    </item>
    <item>
      <title>Item With No Link</title>
      <description>This item has no link and should be skipped.</description>
    </item>
  </channel>
</rss>"""


@pytest.fixture
def rss_fetch_result() -> FetchResult:
    return FetchResult(
        source_id="example_yahoo_finance_rss",
        run_id=str(uuid.uuid4()),
        fetched_at=datetime(2024, 6, 1, 13, 0, tzinfo=timezone.utc),
        url="https://finance.yahoo.com/news/rssindex",
        status_code=200,
        raw_payload=_RSS_FIXTURE,
        content_type="application/rss+xml",
    )


def test_rss_parse_returns_correct_count(rss_fetch_result: FetchResult) -> None:
    """parse() returns one record per <item> with a non-empty link."""
    source = ExampleRSSSource()
    records = source.parse(rss_fetch_result)
    # 2 items have links; the third (no link) is skipped
    assert len(records) == 2


def test_rss_parse_record_fields(rss_fetch_result: FetchResult) -> None:
    """Each record has the expected keys with correct values."""
    source = ExampleRSSSource()
    records = source.parse(rss_fetch_result)
    first = records[0]

    assert first["url"] == "https://finance.yahoo.com/news/apple-earnings-12345.html"
    assert first["title"] == "Apple Reports Record Earnings"
    assert "Apple Inc." in first["body"]
    assert first["published_at"] is not None


def test_rss_parse_published_at_timezone(rss_fetch_result: FetchResult) -> None:
    """published_at has timezone info."""
    source = ExampleRSSSource()
    records = source.parse(rss_fetch_result)
    for r in records:
        assert r["published_at"].tzinfo is not None


def test_rss_source_key(rss_fetch_result: FetchResult) -> None:
    """source_key() returns the article URL."""
    source = ExampleRSSSource()
    records = source.parse(rss_fetch_result)
    assert source.source_key(records[0]) == records[0]["url"]


def test_rss_parse_malformed_xml() -> None:
    """parse() returns empty list on malformed XML without raising."""
    source = ExampleRSSSource()
    bad_result = FetchResult(
        source_id="example_yahoo_finance_rss",
        run_id="r",
        fetched_at=datetime.now(tz=timezone.utc),
        url="https://finance.yahoo.com/news/rssindex",
        status_code=200,
        raw_payload=b"this is not xml <<<",
        content_type="application/rss+xml",
    )
    records = source.parse(bad_result)
    assert records == []


def test_rss_source_id() -> None:
    assert ExampleRSSSource.source_id == "example_yahoo_finance_rss"


def test_rss_schedule() -> None:
    assert ExampleRSSSource.schedule == "0 * * * *"


# ---------------------------------------------------------------------------
# HTML source tests
# ---------------------------------------------------------------------------

# Minimal HN-like HTML to exercise the parser
_HN_FIXTURE = b"""<!DOCTYPE html>
<html>
<body>
<table>
  <tr class="athing">
    <td>
      <span class="titleline">
        <a href="https://example.com/story-1">First Story Title</a>
      </span>
    </td>
  </tr>
  <tr>
    <td>
      <span class="score" id="score_1">142 points</span>
    </td>
  </tr>
  <tr class="athing">
    <td>
      <span class="titleline">
        <a href="https://example.com/story-2">Second Story Title</a>
      </span>
    </td>
  </tr>
  <tr>
    <td>
      <span class="score" id="score_2">88 points</span>
    </td>
  </tr>
</table>
</body>
</html>"""


@pytest.fixture
def html_fetch_result() -> FetchResult:
    return FetchResult(
        source_id="example_hn_frontpage",
        run_id=str(uuid.uuid4()),
        fetched_at=datetime(2024, 6, 1, 14, 0, tzinfo=timezone.utc),
        url="https://news.ycombinator.com",
        status_code=200,
        raw_payload=_HN_FIXTURE,
        content_type="text/html; charset=utf-8",
    )


def test_html_parse_returns_stories(html_fetch_result: FetchResult) -> None:
    """parse() extracts story records from HN HTML."""
    source = ExampleHTMLSource()
    records = source.parse(html_fetch_result)
    assert len(records) >= 2


def test_html_parse_record_fields(html_fetch_result: FetchResult) -> None:
    """Each parsed story has url, title, and fetched_at."""
    source = ExampleHTMLSource()
    records = source.parse(html_fetch_result)
    first = records[0]

    assert "url" in first
    assert "title" in first
    assert "fetched_at" in first
    assert first["url"].startswith("https://")


def test_html_source_key(html_fetch_result: FetchResult) -> None:
    """source_key() returns the story URL."""
    source = ExampleHTMLSource()
    records = source.parse(html_fetch_result)
    assert source.source_key(records[0]) == records[0]["url"]


def test_html_parse_empty_page() -> None:
    """parse() on an empty page returns empty list without raising."""
    source = ExampleHTMLSource()
    empty_result = FetchResult(
        source_id="example_hn_frontpage",
        run_id="r",
        fetched_at=datetime.now(tz=timezone.utc),
        url="https://news.ycombinator.com",
        status_code=200,
        raw_payload=b"<html><body></body></html>",
        content_type="text/html",
    )
    records = source.parse(empty_result)
    assert records == []


def test_html_source_id() -> None:
    assert ExampleHTMLSource.source_id == "example_hn_frontpage"


def test_html_schedule() -> None:
    assert ExampleHTMLSource.schedule == "*/30 * * * *"


def test_html_uses_playwright() -> None:
    assert ExampleHTMLSource.use_playwright is True
