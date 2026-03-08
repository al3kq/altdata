# altdata

Production-quality Python framework for scraping, storing, and scheduling alternative data feeds.

## Why

Alt-data pipelines share the same boilerplate: async HTTP with retry logic, raw payload archival, normalised DB storage, idempotent upserts, and a scheduler to glue it all together.  `altdata` packages that boilerplate into a composable framework so you can focus on the `fetch()` + `parse()` logic for each new source.

## Opinionated stack

| Layer | Choice |
|-------|--------|
| HTTP | `httpx` async client with exponential-backoff retries |
| JS rendering | `playwright` (Chromium, headless) |
| Proxy | Oxylabs residential proxies |
| Database | PostgreSQL via `asyncpg` + SQLAlchemy 2 async ORM |
| Migrations | Alembic (async-compatible) |
| Raw storage | Disk (S3 stub ready) |
| Scheduling | APScheduler asyncio scheduler |
| CLI | Typer |
| Logging | structlog (JSON) |
| Config | Pydantic Settings |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLI (Typer)                              │
│  altdata run <id>  │  altdata list  │  altdata scheduler start   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                    ┌────────▼────────┐
                    │   JobRunner     │  orchestrates one full run
                    └────────┬────────┘
           ┌─────────────────┼──────────────────┐
           ▼                 ▼                  ▼
   ┌───────────────┐  ┌─────────────┐  ┌──────────────┐
   │  BaseSource   │  │  HttpClient │  │  RawStore    │
   │  fetch()      │  │  get()      │  │  save/load   │
   │  parse()      │  │  get_pw()   │  │  disk / S3   │
   └───────────────┘  └──────┬──────┘  └──────────────┘
                             │
                    ┌────────▼────────┐
                    │ ProxyProvider   │
                    │ Oxylabs / Null  │
                    └─────────────────┘
           ┌─────────────────────────────┐
           ▼                             ▼
   ┌───────────────┐           ┌──────────────────┐
   │  RunRepo      │           │  PayloadRepo     │
   │  ScraperRun   │           │  upsert_payload  │
   └───────────────┘           └──────────────────┘
           │                             │
           └──────────┬──────────────────┘
                      ▼
              PostgreSQL (asyncpg)

   ┌─────────────────────────────────────┐
   │         AltDataScheduler            │
   │   APScheduler asyncio               │
   │   register_source() → JobRunner     │
   └─────────────────────────────────────┘
```

## Quick start

```bash
# Install
pip install -e ".[dev]"

# Install Playwright browser
playwright install chromium

# Configure
cp .env.example .env
# Edit DATABASE_URL in .env

# Create tables
altdata db init

# Run a source once
altdata run example_yahoo_finance_rss

# List all sources
altdata list

# View recent runs
altdata runs

# Start the scheduler (runs all sources on their cron schedules)
altdata scheduler start
```

## Writing a new source

Subclass `BaseSource` and implement three methods:

```python
from altdata.core.base_source import BaseSource, FetchResult
from altdata.core.http_client import HttpClient
from typing import Any

class MySource(BaseSource):
    source_id = "my_source"          # unique slug
    schedule  = "0 6 * * *"          # daily at 06:00
    use_proxy = True                  # route through Oxylabs

    async def fetch(self, client: HttpClient) -> list[FetchResult]:
        response = await client.get("https://example.com/data.json")
        return [FetchResult(
            source_id=self.source_id,
            run_id="",               # filled in by JobRunner
            fetched_at=datetime.now(tz=timezone.utc),
            url="https://example.com/data.json",
            status_code=response.status_code,
            raw_payload=response.content,
            content_type=response.headers.get("content-type", ""),
        )]

    def parse(self, result: FetchResult) -> list[dict[str, Any]]:
        import json
        items = json.loads(result.raw_payload)
        return [{"url": item["url"], "title": item["title"], ...} for item in items]

    def source_key(self, record: dict[str, Any]) -> str:
        return record["url"]   # stable dedup key
```

Register it in `altdata/sources/__init__.py`:

```python
from altdata.sources.my_source import MySource

REGISTRY = {
    ...
    MySource.source_id: MySource,
}
```

## Configuration reference

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://...` | SQLAlchemy async DSN |
| `RAW_STORE_BACKEND` | `disk` | `disk` or `s3` |
| `RAW_STORE_PATH` | `~/.altdata/raw` | Root path for disk store |
| `S3_BUCKET` | _(empty)_ | S3 bucket name |
| `S3_PREFIX` | `raw/` | S3 key prefix |
| `OXYLABS_USERNAME` | _(empty)_ | Oxylabs username |
| `OXYLABS_PASSWORD` | _(empty)_ | Oxylabs password |
| `OXYLABS_ENDPOINT` | `pr.oxylabs.io:7777` | Proxy endpoint |
| `HTTP_TIMEOUT` | `30.0` | Request timeout (seconds) |
| `HTTP_MAX_RETRIES` | `3` | Max retry attempts |
| `HTTP_BACKOFF_FACTOR` | `2.0` | Exponential backoff base |
| `PLAYWRIGHT_HEADLESS` | `true` | Run browser headlessly |

## CLI reference

```
altdata run <source_id>        Run a source once immediately
altdata list                   List registered sources and schedules
altdata runs [--source <id>]   Show recent ScraperRuns from the database
altdata db init                Run alembic upgrade head (create tables)
altdata scheduler start        Start the APScheduler loop
```

## Running tests

```bash
pytest -v --cov=altdata
```

Tests use SQLite in-memory and `respx` for httpx mocking — no real network calls or Postgres required.

## Roadmap

- [ ] S3RawStore implementation (aiobotocore)
- [ ] Airflow DAG factory (`AltDataDAG`) — one DAG per registered source
- [ ] DataFrame export: `altdata export <source_id> --format parquet`
- [ ] More sources: SEC EDGAR, Reddit pushshift, satellite imagery metadata
- [ ] Prometheus metrics endpoint
- [ ] Docker Compose dev stack (Postgres + pgAdmin)
