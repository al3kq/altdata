"""Async SQLAlchemy engine and session factory."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

if TYPE_CHECKING:
    from altdata.settings import Settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def init_engine(settings: Settings) -> AsyncEngine:
    """Create and cache the async SQLAlchemy engine.

    Idempotent: returns the same engine if called multiple times with the
    same settings.

    Args:
        settings: Application settings containing the database URL.

    Returns:
        The configured AsyncEngine instance.
    """
    global _engine, _session_factory
    if _engine is None:
        _engine = create_async_engine(
            settings.database_url,
            echo=False,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
        _session_factory = async_sessionmaker(
            bind=_engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
    return _engine


async def get_session(settings: Settings) -> AsyncGenerator[AsyncSession, None]:
    """Async generator that yields a database session.

    Intended for use with ``async with`` or FastAPI-style dependency injection.
    Commits on clean exit, rolls back on exception.

    Args:
        settings: Application settings (used to initialise the engine if needed).

    Yields:
        An AsyncSession instance.
    """
    init_engine(settings)
    assert _session_factory is not None
    async with _session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def get_session_factory(settings: Settings) -> async_sessionmaker[AsyncSession]:
    """Return the session factory, initialising the engine if necessary.

    Args:
        settings: Application settings.

    Returns:
        The async_sessionmaker instance.
    """
    init_engine(settings)
    assert _session_factory is not None
    return _session_factory
