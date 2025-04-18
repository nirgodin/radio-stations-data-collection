from datetime import datetime, timedelta

from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.util import undefined
from typing_extensions import Optional

from data_collectors.components import ComponentFactory
from data_collectors.jobs.base_job_builder import BaseJobBuilder
from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ScheduledJob
from data_collectors.utils.datetime import random_upcoming_time


class StatusReporterJobBuilder(BaseJobBuilder):
    def __init__(
        self,
        component_factory: ComponentFactory = ComponentFactory(),
        lookback_period: Optional[timedelta] = None,
    ):
        super().__init__(component_factory)
        self._lookback_period = lookback_period or timedelta(days=7)

    async def build(self, next_run_time: Optional[datetime] = undefined) -> ScheduledJob:
        return ScheduledJob(
            task=self._task,
            id=JobId.STATUS_REPORTER,
            interval=IntervalTrigger(days=7),
            next_run_time=next_run_time or random_upcoming_time(),
        )

    async def _task(self) -> None:
        manager = self._component_factory.misc.get_status_reporter_manager()
        await manager.run(self._lookback_period)
