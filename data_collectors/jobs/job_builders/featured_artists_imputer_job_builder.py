from datetime import datetime

from apscheduler.triggers.interval import IntervalTrigger
from typing_extensions import Optional

from data_collectors.jobs.base_job_builder import BaseJobBuilder
from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ScheduledJob


class FeaturedArtistsImputerJobBuilder(BaseJobBuilder):
    async def build(self, next_run_time: Optional[datetime] = None) -> ScheduledJob:
        return ScheduledJob(
            task=self._task,
            id=JobId.FEATURED_ARTISTS_IMPUTER,
            interval=IntervalTrigger(hours=3),
            next_run_time=next_run_time,
        )

    async def _task(self) -> None:
        async with self._component_factory.sessions.get_spotify_session() as spotify_session:
            manager = self._component_factory.spotify.get_spotify_featured_artists_imputer_manager(spotify_session)
            await manager.run()
