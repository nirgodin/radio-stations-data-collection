from datetime import datetime

from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.util import undefined
from typing_extensions import Optional

from data_collectors.jobs.base_job_builder import BaseJobBuilder
from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ScheduledJob
from data_collectors.utils.datetime import random_upcoming_time


class SpotifyArtistsAboutsJobBuilder(BaseJobBuilder):
    async def build(self, next_run_time: Optional[datetime] = undefined) -> ScheduledJob:
        return ScheduledJob(
            task=self._task,
            id=JobId.SPOTIFY_ARTISTS_ABOUTS,
            interval=IntervalTrigger(hours=6),
            next_run_time=random_upcoming_time(),
        )

    async def _task(self) -> None:
        async with self._component_factory.sessions.enter_browser_session() as browser:
            manager = self._component_factory.spotify.get_artists_about_manager(browser)
            await manager.run(limit=10)
