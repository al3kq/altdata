"""Built-in example sources shipped with the altdata framework."""

from altdata.sources.example_html import ExampleHTMLSource
from altdata.sources.example_rss import ExampleRSSSource

# Registry of all known sources — add new sources here
REGISTRY: dict[str, type] = {
    ExampleRSSSource.source_id: ExampleRSSSource,
    ExampleHTMLSource.source_id: ExampleHTMLSource,
}

__all__ = ["ExampleRSSSource", "ExampleHTMLSource", "REGISTRY"]
