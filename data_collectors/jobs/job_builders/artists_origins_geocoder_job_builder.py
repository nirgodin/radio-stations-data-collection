from datetime import datetime

from apscheduler.triggers.interval import IntervalTrigger
from typing_extensions import Optional

from data_collectors.jobs.base_job_builder import BaseJobBuilder
from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ScheduledJob
from data_collectors.utils.datetime import random_upcoming_time


class ArtistsOriginsGeocoderJobBuilder(BaseJobBuilder):
    async def build(self, next_run_time: Optional[datetime] = None) -> ScheduledJob:
        return ScheduledJob(
            task=self._task,
            id=JobId.ARTISTS_GEOCODING,
            interval=IntervalTrigger(days=1),
            next_run_time=next_run_time or random_upcoming_time(),
        )

    async def _task(self) -> None:
        async with self._component_factory.sessions.get_client_session() as client_session:
            manager = self._component_factory.google.get_artists_origin_geocoding_manager(client_session)
            await manager.run(limit=100)
