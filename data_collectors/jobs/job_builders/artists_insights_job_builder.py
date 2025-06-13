from datetime import datetime

from apscheduler.triggers.interval import IntervalTrigger
from typing_extensions import Optional

from data_collectors.jobs.base_job_builder import BaseJobBuilder
from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ScheduledJob


class ArtistsInsightsJobBuilder(BaseJobBuilder):
    async def build(self, next_run_time: Optional[datetime] = None) -> ScheduledJob:
        return ScheduledJob(
            task=self._task,
            id=JobId.ARTISTS_INSIGHTS,
            interval=IntervalTrigger(hours=12),
            next_run_time=next_run_time,
        )

    async def _task(self) -> None:
        manager = self._component_factory.google.get_wikipedia_artists_about_manager()
        await manager.run(limit=100)
