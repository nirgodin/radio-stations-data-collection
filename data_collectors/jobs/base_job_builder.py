from abc import ABC, abstractmethod

from data_collectors.components import ComponentFactory
from data_collectors.logic.models import ScheduledJob


class BaseJobBuilder(ABC):
    def __init__(self, component_factory: ComponentFactory = ComponentFactory()):
        self._component_factory = component_factory

    @abstractmethod
    async def build(self) -> ScheduledJob:
        raise NotImplementedError
