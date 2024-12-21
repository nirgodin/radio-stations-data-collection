from functools import lru_cache
from typing import Dict

from data_collectors.components import ComponentFactory
from data_collectors.jobs.jobs_loader import JobsLoader
from data_collectors.logic.models import ScheduledJob


@lru_cache
def get_component_factory() -> ComponentFactory:
    return ComponentFactory()


async def get_jobs_map() -> Dict[str, ScheduledJob]:
    component_factory = get_component_factory()
    return await JobsLoader.load(component_factory, "jobs")
