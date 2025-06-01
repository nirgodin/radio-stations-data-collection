from datetime import datetime

from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.util import undefined
from typing_extensions import Optional

from data_collectors.jobs.base_job_builder import BaseJobBuilder
from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ScheduledJob
from data_collectors.utils.datetime import random_upcoming_time


class WikipediaSummariesJobBuilder(BaseJobBuilder):
    async def build(self, next_run_time: Optional[datetime] = undefined) -> ScheduledJob:
        return ScheduledJob(
            task=self._task,
            id=JobId.WIKIPEDIA_SUMMARIES,
            interval=IntervalTrigger(days=1),
            next_run_time=next_run_time or random_upcoming_time(),
        )

    async def _task(self) -> None:
        manager = await self._component_factory.wikipedia.get_artists_about_manager()
        await manager.run(100)
