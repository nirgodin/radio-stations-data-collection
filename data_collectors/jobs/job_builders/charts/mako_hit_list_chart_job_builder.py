from datetime import datetime

from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.util import undefined
from typing_extensions import Optional

from data_collectors.jobs.base_job_builder import BaseJobBuilder
from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ScheduledJob


class MakoHitListChartJobBuilder(BaseJobBuilder):
    async def build(self, next_run_time: Optional[datetime] = undefined) -> ScheduledJob:
        return ScheduledJob(
            task=self._task,
            id=JobId.MAKO_HIT_LIST,
            interval=IntervalTrigger(weeks=1),
            next_run_time=next_run_time or None,
        )

    async def _task(self) -> None:
        async with self._component_factory.sessions.enter_spotify_session() as session:
            manager = self._component_factory.charts.get_mako_hit_list_charts_manager(session)
            await manager.run()
