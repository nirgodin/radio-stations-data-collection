from datetime import datetime, timedelta

from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.util import undefined
from genie_datastores.postgres.models import ShazamTopTrack
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select, func
from typing_extensions import Optional

from data_collectors.jobs.base_job_builder import BaseJobBuilder
from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ScheduledJob


class ShazamTopTracksJobBuilder(BaseJobBuilder):
    async def build(self, next_run_time: Optional[datetime] = undefined) -> ScheduledJob:
        return ScheduledJob(
            task=self._task,
            id=JobId.SHAZAM_TOP_TRACKS,
            interval=IntervalTrigger(days=1),
            next_run_time=next_run_time or await self._calculate_next_run_time(),
        )

    async def _task(self) -> None:
        manager = await self._component_factory.shazam.get_top_tracks_manager()
        await manager.run()

    async def _calculate_next_run_time(self) -> datetime:
        engine = self._component_factory.tools.get_database_engine()
        query = select(func.max(ShazamTopTrack.date)).limit(1)
        query_result = await execute_query(engine, query)
        latest_entry_date = query_result.scalars().first()
        today = datetime.today()

        if self._should_execute_job_today(today, latest_entry_date):
            return today

        return today + timedelta(days=1)

    @staticmethod
    def _should_execute_job_today(today: datetime, latest_entry_date: Optional[datetime]) -> bool:
        if latest_entry_date is None:
            return True

        return latest_entry_date.date() < today.date()
