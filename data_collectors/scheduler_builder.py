from datetime import timedelta
from functools import partial
from random import randint
from typing import Dict, Optional, List

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from genie_common.tools import logger, EmailSender

from data_collectors.components import ComponentFactory
from data_collectors.jobs.jobs_loader import JobsLoader
from data_collectors.logic.models import ScheduledJob


class SchedulerBuilder:
    def __init__(self, component_factory: ComponentFactory):
        self._component_factory = component_factory

    async def build(self, scheduler: AsyncIOScheduler, jobs: Optional[List[str]]) -> AsyncIOScheduler:
        if jobs is None:
            jobs = await JobsLoader.load(self._component_factory)

        logger.info(f"Found {len(jobs)} jobs to schedule")
        self._add_all_jobs(scheduler, jobs)
        logger.info("Added all jobs to the scheduler")

        return scheduler

    def _add_all_jobs(self, scheduler: AsyncIOScheduler, jobs: Dict[str, ScheduledJob]) -> None:
        email_sender = self._component_factory.tools.get_email_sender()

        for job_name, job in jobs.items():
            func = partial(self._task_with_failure_notification, email_sender, job)
            scheduler.add_job(
                name=job_name,
                func=func,
                trigger=job.interval,
                id=job.id.value,
                next_run_time=job.next_run_time,
            )

    @staticmethod
    def _random_short_delay() -> timedelta:
        return timedelta(minutes=randint(1, 3))

    @staticmethod
    async def _task_with_failure_notification(email_sender: EmailSender, job: ScheduledJob) -> None:
        try:
            with email_sender.notify_failure():
                await job.task()

        except Exception:
            logger.exception(f"Failed executing job `{job.id.value}`")
