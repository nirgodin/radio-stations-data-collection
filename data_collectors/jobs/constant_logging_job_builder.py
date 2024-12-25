from apscheduler.triggers.interval import IntervalTrigger
from beanie.migrations.runner import logger

from data_collectors.jobs.base_job_builder import BaseJobBuilder
from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ScheduledJob


class ConstantLoggingJobBuilder(BaseJobBuilder):
    async def build(self):
        return ScheduledJob(
            id=JobId.CONSTANT_LOGGER,
            task=self._task,
            interval=IntervalTrigger(minutes=5)
        )

    @staticmethod
    async def _task() -> None:
        logger.info("I'm still breathing")
