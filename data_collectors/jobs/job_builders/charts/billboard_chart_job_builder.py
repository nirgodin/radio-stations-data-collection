from datetime import datetime

from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.util import undefined
from typing_extensions import Optional

from data_collectors.jobs.base_job_builder import BaseJobBuilder
from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ScheduledJob
from data_collectors.utils.datetime import random_upcoming_time


class BillboardChartJobBuilder(BaseJobBuilder):
    async def build(self, next_run_time: Optional[datetime] = undefined) -> ScheduledJob:
        return ScheduledJob(
            task=self._task,
            id=JobId.BILLBOARD_CHARTS,
            interval=IntervalTrigger(hours=12),
            next_run_time=next_run_time or random_upcoming_time(),
        )

    async def _task(self) -> None:
        async with self._component_factory.sessions.enter_spotify_session() as spotify_session:
            async with self._component_factory.sessions.get_client_session() as client_session:
                manager = self._component_factory.charts.get_billboard_charts_manager(client_session, spotify_session)
                await manager.run()
