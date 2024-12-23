import os
from importlib import import_module
from importlib.util import spec_from_file_location, module_from_spec
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
        builders_dir = os.path.dirname(__file__)

        for filename in os.listdir(builders_dir):
            logger.info(f"Loading {filename}")

            if JobsLoader._is_python_file(filename):
                module_name = os.path.splitext(filename)[0]
                module_path = os.path.join(builders_dir, filename)
                spec = spec_from_file_location(module_name, module_path)

                if spec.loader is not None:
                    module = module_from_spec(spec)
                    spec.loader.exec_module(module)

    @staticmethod
    def _is_python_file(filename: str) -> bool:
        return filename.endswith(".py") and filename != "__init__.py"
