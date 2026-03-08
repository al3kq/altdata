"""End-to-end orchestration of a single source scraping run."""

from __future__ import annotations

import asyncio
import traceback
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import structlog

from altdata.core.http_client import HttpClient
from altdata.core.proxy import NullProxyProvider, OxylabsProxyProvider
from altdata.core.raw_store import get_raw_store
from altdata.db.repos.payload_repo import PayloadRepo
from altdata.db.repos.run_repo import RunRepo
from altdata.db.session import get_session_factory
from altdata.settings import get_settings

if TYPE_CHECKING:
    from altdata.core.base_source import BaseSource, FetchResult
    from altdata.settings import Settings

logger = structlog.get_logger(__name__)


@dataclass
class RunSummary:
    """Summary of a completed JobRunner invocation.

    Attributes:
        run_id: UUID string of the ScraperRun created in the database.
        source_id: The source slug that was run.
        status: Final status: ``"success"`` or ``"failed"``.
        records_fetched: Number of FetchResult objects returned by fetch().
        records_upserted: Number of Payload rows inserted or updated.
        raw_store_paths: List of paths written to the RawStore.
        error_message: Error description if status is ``"failed"``.
    """

    run_id: str
    source_id: str
    status: str
    records_fetched: int = 0
    records_upserted: int = 0
    raw_store_paths: list[str] = field(default_factory=list)
    error_message: str | None = None


class JobRunner:
    """Orchestrates a full end-to-end source run.

    Steps executed for each run:

    1. Create a ``ScraperRun`` row in the database (``status="running"``).
    2. Build an ``HttpClient`` with or without a proxy (based on ``source.use_proxy``).
    3. Call ``source.fetch(client)`` to obtain raw HTTP results.
    4. Save each ``FetchResult`` to the ``RawStore``.
    5. Call ``source.parse(result)`` to obtain normalised records.
    6. Upsert each record via ``PayloadRepo``.
    7. Mark the run as ``"success"`` or ``"failed"`` in the database.
    8. Return a ``RunSummary``.

    Args:
        settings: Application settings.  Uses the global singleton if omitted.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()

    async def run(self, source: BaseSource) -> RunSummary:
        """Execute a source end-to-end.

        Args:
            source: The BaseSource subclass instance to run.

        Returns:
            A RunSummary describing the outcome of the run.
        """
        log = logger.bind(source_id=source.source_id)
        run_id = str(uuid.uuid4())
        log = log.bind(run_id=run_id)

        session_factory = get_session_factory(self._settings)
        raw_store = get_raw_store(self._settings)

        async with session_factory() as session:
            run_repo = RunRepo(session)
            payload_repo = PayloadRepo(session)

            scraper_run = await run_repo.create_run(source.source_id)
            # Use the DB-assigned UUID as the canonical run_id
            run_id = str(scraper_run.id)
            log = log.bind(run_id=run_id)
            await session.commit()

        log.info("job_runner_started")

        raw_store_paths: list[str] = []
        records_fetched = 0
        records_upserted = 0

        try:
            # Build the HTTP client
            proxy_provider = (
                OxylabsProxyProvider(self._settings)
                if source.use_proxy
                else NullProxyProvider()
            )

            async with HttpClient(self._settings, proxy_provider) as client:
                # Inject run_id into each FetchResult before saving
                fetch_results = await source.fetch(client)

                for result in fetch_results:
                    result.run_id = run_id
                    records_fetched += 1

                    # Persist raw payload
                    saved_path = await raw_store.save(result)
                    raw_store_paths.append(str(saved_path))
                    log.debug("raw_payload_saved", path=str(saved_path))

                    # Parse and upsert records
                    parsed_records = source.parse(result)
                    upsert_tasks = [
                        self._upsert_record(
                            source=source,
                            record=record,
                            raw_store_path=str(saved_path),
                            session_factory=session_factory,
                        )
                        for record in parsed_records
                    ]
                    results = await asyncio.gather(*upsert_tasks, return_exceptions=True)
                    for r in results:
                        if isinstance(r, Exception):
                            log.warning("upsert_error", error=str(r))
                        else:
                            records_upserted += 1

            # Mark success
            async with session_factory() as session:
                run_repo = RunRepo(session)
                await run_repo.complete_run(
                    run_id=scraper_run.id,
                    records_fetched=records_fetched,
                    records_upserted=records_upserted,
                    paths=raw_store_paths,
                )
                await session.commit()

            log.info(
                "job_runner_completed",
                records_fetched=records_fetched,
                records_upserted=records_upserted,
            )
            return RunSummary(
                run_id=run_id,
                source_id=source.source_id,
                status="success",
                records_fetched=records_fetched,
                records_upserted=records_upserted,
                raw_store_paths=raw_store_paths,
            )

        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            log.error("job_runner_failed", error=str(exc))

            try:
                async with session_factory() as session:
                    run_repo = RunRepo(session)
                    await run_repo.fail_run(
                        run_id=scraper_run.id,
                        error_message=error_msg,
                    )
                    await session.commit()
            except Exception as db_exc:
                log.error("failed_to_persist_failure", error=str(db_exc))

            return RunSummary(
                run_id=run_id,
                source_id=source.source_id,
                status="failed",
                records_fetched=records_fetched,
                records_upserted=records_upserted,
                raw_store_paths=raw_store_paths,
                error_message=error_msg,
            )

    async def _upsert_record(
        self,
        source: BaseSource,
        record: dict[str, Any],
        raw_store_path: str,
        session_factory: Any,
    ) -> bool:
        """Upsert a single parsed record and return True on success."""
        record = dict(record)
        record["raw_store_path"] = raw_store_path
        source_key = source.source_key(record)

        async with session_factory() as session:
            repo = PayloadRepo(session)
            _, was_inserted = await repo.upsert_payload(
                source_id=source.source_id,
                source_key=source_key,
                data=record,
            )
            await session.commit()
        return was_inserted
