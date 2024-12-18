import os
from importlib.util import spec_from_file_location, module_from_spec
from typing import List

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from genie_common.tools import logger

from data_collectors.components import ComponentFactory
from data_collectors.jobs.base_job_builder import BaseJobBuilder
from data_collectors.logic.models import ScheduledJob


class SchedulerBuilder:
    def __init__(self, component_factory: ComponentFactory = ComponentFactory(), builders_dir: str = "jobs"):
        self._component_factory = component_factory
        self._builders_dir = builders_dir

    async def build(self) -> AsyncIOScheduler:
        scheduler = AsyncIOScheduler()
        jobs = await self._load_all_jobs()
        logger.info(f"Found {len(jobs)} jobs to schedule")

        for job in jobs:
            scheduler.add_job(
                func=job.task,
                trigger=job.interval,
                id=job.id,
            )

        logger.info(f"Added all jobs to the scheduler")
        return scheduler

    async def _load_all_jobs(self) -> List[ScheduledJob]:
        logger.info("Loading all scheduled jobs dynamically")
        self._import_all_job_builders()
        jobs = []

        for builder_class in BaseJobBuilder.__subclasses__():
            builder = builder_class(component_factory=self._component_factory)
            job = await builder.build()
            jobs.append(job)

        return jobs

    def _import_all_job_builders(self) -> None:
        for filename in os.listdir(self._builders_dir):
            if self._is_python_file(filename):
                module_name = os.path.splitext(filename)[0]
                module_path = os.path.join(self._builders_dir, filename)
                spec = spec_from_file_location(module_name, module_path)
                module = module_from_spec(spec)
                spec.loader.exec_module(module)

    @staticmethod
    def _is_python_file(filename: str) -> bool:
        return filename.endswith(".py") and filename != "__init__.py"
