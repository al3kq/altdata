"""SQLAlchemy ORM models for the altdata framework."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Shared declarative base for all altdata ORM models."""


class ScraperRun(Base):
    """Records a single execution of a scraping source.

    Each row captures the lifecycle of one JobRunner invocation: when it
    started, when it finished, its final status, how many records were
    fetched and upserted, any error message, and the list of raw store
    paths written during the run.
    """

    __tablename__ = "scraper_runs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
        default=uuid.uuid4,
    )
    source_id: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="running"
    )  # "running" | "success" | "failed"
    records_fetched: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    records_upserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_store_paths: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)

    def __repr__(self) -> str:
        return (
            f"<ScraperRun id={self.id} source_id={self.source_id!r} status={self.status!r}>"
        )


class Payload(Base):
    """A normalised, deduplicated record produced by a scraping source.

    The ``(source_id, source_key)`` pair is the unique constraint used for
    idempotent upserts.  Raw bytes are NOT stored here — only the path back
    to the RawStore.
    """

    __tablename__ = "payloads"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
        default=uuid.uuid4,
    )
    source_id: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    source_key: Mapped[str] = mapped_column(String(512), nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    body: Mapped[str | None] = mapped_column(Text, nullable=True)
    data: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    raw_store_path: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("source_id", "source_key", name="uq_payload_source_key"),
    )

    def __repr__(self) -> str:
        return (
            f"<Payload id={self.id} source_id={self.source_id!r} "
            f"source_key={self.source_key!r}>"
        )
