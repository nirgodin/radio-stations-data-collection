from abc import ABC, abstractmethod
from datetime import datetime

from typing_extensions import Optional

from data_collectors.components import ComponentFactory
from data_collectors.logic.models import ScheduledJob


class BaseJobBuilder(ABC):
    def __init__(self, component_factory: ComponentFactory = ComponentFactory()):
        self._component_factory = component_factory

    @abstractmethod
    async def build(self, next_run_time: Optional[datetime] = None) -> ScheduledJob:
        raise NotImplementedError
