from abc import ABC, abstractmethod
from typing import Any, List

from genie_datastores.postgres.models import ChartEntry

from data_collectors.contract.collector_interface import ICollector


class IChartsDataCollector(ICollector, ABC):
    @abstractmethod
    async def collect(self, *args, **kwargs) -> List[ChartEntry]:
        raise NotImplementedError
