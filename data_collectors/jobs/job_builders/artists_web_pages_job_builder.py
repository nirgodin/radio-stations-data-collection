from datetime import datetime

from apscheduler.triggers.interval import IntervalTrigger
from typing_extensions import Optional

from data_collectors.jobs.base_job_builder import BaseJobBuilder
from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ScheduledJob
from data_collectors.utils.datetime import random_upcoming_time


class ArtistsWebPagesJobBuilder(BaseJobBuilder):
    async def build(self, next_run_time: Optional[datetime] = None) -> ScheduledJob:
        return ScheduledJob(
            task=self._task,
            id=JobId.ARTISTS_WEB_PAGES,
            interval=IntervalTrigger(hours=12),
            next_run_time=next_run_time or random_upcoming_time(),
        )

    async def _task(self) -> None:
        async with self._component_factory.sessions.get_client_session() as session:
            manager = self._component_factory.google.get_artists_web_pages_manager(session)
            await manager.run(limit=50)
