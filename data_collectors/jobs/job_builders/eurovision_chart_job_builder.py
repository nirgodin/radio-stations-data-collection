from datetime import datetime

from apscheduler.triggers.interval import IntervalTrigger
from typing_extensions import Optional

from data_collectors.jobs.base_job_builder import BaseJobBuilder
from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ScheduledJob


class EurovisionChartJobBuilder(BaseJobBuilder):
    async def build(self, next_run_time: Optional[datetime] = None) -> ScheduledJob:
        return ScheduledJob(
            task=self._task,
            id=JobId.EUROVISION_CHARTS,
            interval=IntervalTrigger(days=365),
            next_run_time=next_run_time,
        )

    async def _task(self) -> None:
        async with self._component_factory.sessions.get_spotify_session() as spotify_session:
            async with self._component_factory.sessions.get_client_session() as client_session:
                manager = self._component_factory.charts.get_eurovision_charts_manager(
                    client_session=client_session,
                    spotify_session=spotify_session,
                )
                await manager.run()
