"""Upsert and query operations for Payload records."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from altdata.db.models import Payload


class PayloadRepo:
    """Repository for Payload upsert and retrieval.

    Uses PostgreSQL's ``INSERT ... ON CONFLICT DO UPDATE`` for idempotent
    upserts keyed on ``(source_id, source_key)``.

    Args:
        session: An active AsyncSession instance.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert_payload(
        self,
        source_id: str,
        source_key: str,
        data: dict[str, Any],
    ) -> tuple[Payload, bool]:
        """Upsert a Payload row using conflict-safe INSERT.

        On conflict (source_id, source_key) the row is updated with the
        latest values.  The boolean return value indicates whether a new
        row was inserted (True) or an existing row was updated (False).

        Args:
            source_id: Source slug identifying the originating source.
            source_key: Stable deduplication key from ``BaseSource.source_key()``.
            data: Normalised dict containing payload fields.  Expected keys:
                ``url``, ``title``, ``body``, ``fetched_at``, ``published_at``,
                ``raw_store_path``, and any extra fields (stored in ``data``).

        Returns:
            Tuple of (Payload instance, was_inserted).
        """
        now = datetime.now(tz=timezone.utc)
        new_id = uuid.uuid4()

        # Extract known columns; everything else goes into the JSONB data blob
        url = data.get("url", "")
        title = data.get("title")
        body = data.get("body")
        fetched_at = data.get("fetched_at", now)
        published_at = data.get("published_at")
        raw_store_path = data.get("raw_store_path", "")
        extra_data = {
            k: v
            for k, v in data.items()
            if k not in {"url", "title", "body", "fetched_at", "published_at", "raw_store_path"}
        }

        stmt = (
            pg_insert(Payload)
            .values(
                id=new_id,
                source_id=source_id,
                source_key=source_key,
                fetched_at=fetched_at,
                published_at=published_at,
                url=url,
                title=title,
                body=body,
                data=extra_data,
                raw_store_path=raw_store_path,
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_update(
                constraint="uq_payload_source_key",
                set_={
                    "fetched_at": fetched_at,
                    "published_at": published_at,
                    "url": url,
                    "title": title,
                    "body": body,
                    "data": extra_data,
                    "raw_store_path": raw_store_path,
                    "updated_at": now,
                },
            )
            .returning(Payload.id, Payload.created_at, Payload.updated_at)
        )

        result = await self._session.execute(stmt)
        row = result.fetchone()
        was_inserted = row.created_at == row.updated_at  # heuristic; same-second upserts are rare

        # Re-fetch the full ORM object
        payload_result = await self._session.execute(
            select(Payload).where(Payload.id == row.id)
        )
        payload = payload_result.scalar_one()
        return payload, was_inserted

    async def get_latest(self, source_id: str, limit: int = 100) -> list[Payload]:
        """Return the most recently fetched Payloads for a given source.

        Args:
            source_id: Source slug to filter by.
            limit: Maximum number of rows to return.

        Returns:
            List of Payload objects ordered by fetched_at descending.
        """
        result = await self._session.execute(
            select(Payload)
            .where(Payload.source_id == source_id)
            .order_by(Payload.fetched_at.desc())
            .limit(limit)
        )
        return list(result.scalars().all())
