"""APScheduler wrapper for periodic source execution.

Scheduling logic lives here; no scraping logic is present.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

if TYPE_CHECKING:
    from altdata.core.base_source import BaseSource
    from altdata.settings import Settings

logger = structlog.get_logger(__name__)

# Matches interval strings like "30m", "1h", "2h30m" is NOT supported (just one unit)
_INTERVAL_RE = re.compile(r"^(\d+)(s|m|h|d)$")


def _parse_trigger(schedule: str) -> Any:
    """Parse a schedule string into an APScheduler trigger.

    Supports:
    - Cron expressions: ``"0 * * * *"`` (5-field standard cron).
    - Interval shorthand: ``"30s"``, ``"5m"``, ``"1h"``, ``"2d"``.

    Args:
        schedule: Schedule string from a BaseSource subclass.

    Returns:
        An APScheduler CronTrigger or IntervalTrigger.

    Raises:
        ValueError: If the schedule string cannot be parsed.
    """
    # Try interval shorthand first
    m = _INTERVAL_RE.match(schedule.strip())
    if m:
        value = int(m.group(1))
        unit = m.group(2)
        unit_map = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}
        return IntervalTrigger(**{unit_map[unit]: value})

    # Fall back to cron expression (must have exactly 5 fields)
    parts = schedule.strip().split()
    if len(parts) == 5:
        return CronTrigger.from_crontab(schedule.strip())

    raise ValueError(
        f"Cannot parse schedule {schedule!r}. "
        "Use a 5-field cron expression or an interval like '30m', '1h', '2d'."
    )


class AltDataScheduler:
    """Thin wrapper around APScheduler for running altdata sources on a schedule.

    Sources are registered individually via ``register_source()``.  The
    scheduler delegates execution to ``JobRunner.run()`` — no scraping
    logic lives here.

    Args:
        settings: Application settings passed through to JobRunner.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        from altdata.settings import get_settings

        self._settings = settings or get_settings()
        self._scheduler = AsyncIOScheduler()

    def register_source(self, source: BaseSource) -> None:
        """Register a source with the scheduler.

        Parses the source's schedule string and adds a job that calls
        ``JobRunner.run(source)`` on each trigger.

        Args:
            source: A configured BaseSource subclass instance.

        Raises:
            ValueError: If the source's schedule string cannot be parsed.
        """
        trigger = _parse_trigger(source.schedule)
        job_id = f"altdata_{source.source_id}"

        self._scheduler.add_job(
            self._run_source,
            trigger=trigger,
            id=job_id,
            args=[source],
            replace_existing=True,
            misfire_grace_time=60,
        )
        logger.info(
            "source_registered",
            source_id=source.source_id,
            schedule=source.schedule,
            job_id=job_id,
        )

    async def _run_source(self, source: BaseSource) -> None:
        """Job callback: create a JobRunner and execute the source.

        Args:
            source: The source to run.
        """
        from altdata.core.job_runner import JobRunner

        runner = JobRunner(self._settings)
        summary = await runner.run(source)
        logger.info(
            "scheduled_run_complete",
            source_id=source.source_id,
            status=summary.status,
            records_fetched=summary.records_fetched,
            records_upserted=summary.records_upserted,
        )

    def start(self) -> None:
        """Start the APScheduler event loop.

        Call this from within a running asyncio event loop (e.g. via
        ``asyncio.run()`` or inside an async context).
        """
        self._scheduler.start()
        logger.info("scheduler_started", job_count=len(self._scheduler.get_jobs()))

    def stop(self, wait: bool = True) -> None:
        """Stop the scheduler gracefully.

        Args:
            wait: If True, waits for currently running jobs to finish.
        """
        self._scheduler.shutdown(wait=wait)
        logger.info("scheduler_stopped")

    def list_jobs(self) -> list[dict[str, Any]]:
        """Return a summary of all registered jobs.

        Returns:
            List of dicts with ``id``, ``next_run_time``, and ``trigger`` keys.
        """
        return [
            {
                "id": job.id,
                "next_run_time": str(job.next_run_time),
                "trigger": str(job.trigger),
            }
            for job in self._scheduler.get_jobs()
        ]
