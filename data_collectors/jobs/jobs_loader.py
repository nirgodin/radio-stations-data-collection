import os
from importlib import import_module
from typing import Dict

from async_lru import alru_cache
from genie_common.tools import logger

from data_collectors.components import ComponentFactory
from data_collectors.jobs.base_job_builder import BaseJobBuilder
from data_collectors.logic.models import ScheduledJob


class JobsLoader:
    @staticmethod
    @alru_cache
    async def load(component_factory: ComponentFactory) -> Dict[str, ScheduledJob]:
        logger.info("Loading all scheduled jobs dynamically")
        JobsLoader._import_all_job_builders()
        jobs = {}

        for builder_class in BaseJobBuilder.__subclasses__():
            builder = builder_class(component_factory=component_factory)
            job = await builder.build()
            jobs[job.id.value] = job

        return jobs

    @staticmethod
    def _import_all_job_builders() -> None:
        builders_dir = os.path.relpath(os.path.dirname(__file__))

        for dir_path, dir_names, filenames in os.walk(builders_dir):
            dir_module = JobsLoader._get_builders_module(dir_path)

            for filename in filenames:
                if JobsLoader._is_python_file(filename):
                    logger.info(f"Loading {filename}")
                    module_name = os.path.splitext(filename)[0]
                    import_module(f"{dir_module}.{module_name}")

    @staticmethod
    def _get_builders_module(builders_dir: str) -> str:
        split_dir_path = builders_dir.split(os.path.sep)
        return ".".join(split_dir_path)

    @staticmethod
    def _is_python_file(filename: str) -> bool:
        return filename.endswith(".py") and filename != "__init__.py"
