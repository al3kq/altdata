"""CRUD operations for ScraperRun records."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altdata.db.models import ScraperRun


class RunRepo:
    """Repository for ScraperRun CRUD operations.

    Args:
        session: An active AsyncSession instance.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_run(self, source_id: str) -> ScraperRun:
        """Create a new ScraperRun with status='running'.

        Args:
            source_id: The source slug for this run.

        Returns:
            The newly created and persisted ScraperRun.
        """
        run = ScraperRun(
            id=uuid.uuid4(),
            source_id=source_id,
            started_at=datetime.now(tz=timezone.utc),
            status="running",
            records_fetched=0,
            records_upserted=0,
            raw_store_paths=[],
        )
        self._session.add(run)
        await self._session.flush()
        return run

    async def complete_run(
        self,
        run_id: uuid.UUID | str,
        records_fetched: int,
        records_upserted: int,
        paths: list[str],
    ) -> ScraperRun:
        """Mark a ScraperRun as successfully completed.

        Args:
            run_id: UUID of the run to update.
            records_fetched: Total number of FetchResult objects processed.
            records_upserted: Total number of Payload rows upserted.
            paths: List of raw store path strings written during the run.

        Returns:
            The updated ScraperRun.

        Raises:
            ValueError: If the run is not found.
        """
        run = await self._get_run(run_id)
        run.finished_at = datetime.now(tz=timezone.utc)
        run.status = "success"
        run.records_fetched = records_fetched
        run.records_upserted = records_upserted
        run.raw_store_paths = paths
        await self._session.flush()
        return run

    async def fail_run(self, run_id: uuid.UUID | str, error_message: str) -> ScraperRun:
        """Mark a ScraperRun as failed with an error message.

        Args:
            run_id: UUID of the run to update.
            error_message: Human-readable description of what went wrong.

        Returns:
            The updated ScraperRun.

        Raises:
            ValueError: If the run is not found.
        """
        run = await self._get_run(run_id)
        run.finished_at = datetime.now(tz=timezone.utc)
        run.status = "failed"
        run.error_message = error_message
        await self._session.flush()
        return run

    async def list_runs(self, source_id: str | None = None, limit: int = 50) -> list[ScraperRun]:
        """Return recent ScraperRuns ordered by start time descending.

        Args:
            source_id: Optional filter by source slug.
            limit: Maximum number of rows to return.

        Returns:
            List of ScraperRun objects.
        """
        stmt = select(ScraperRun).order_by(ScraperRun.started_at.desc()).limit(limit)
        if source_id is not None:
            stmt = stmt.where(ScraperRun.source_id == source_id)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def _get_run(self, run_id: uuid.UUID | str) -> ScraperRun:
        """Fetch a ScraperRun by its primary key.

        Raises:
            ValueError: If no run with the given ID exists.
        """
        if isinstance(run_id, str):
            run_id = uuid.UUID(run_id)
        result = await self._session.execute(
            select(ScraperRun).where(ScraperRun.id == run_id)
        )
        run = result.scalar_one_or_none()
        if run is None:
            raise ValueError(f"ScraperRun {run_id} not found")
        return run
