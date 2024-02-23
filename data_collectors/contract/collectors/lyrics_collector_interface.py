from abc import abstractmethod, ABC
from typing import List, Dict

from data_collectors.contract.collector_interface import ICollector


class ILyricsCollector(ICollector, ABC):
    @abstractmethod
    async def collect(self, ids: List[str]) -> Dict[str, List[str]]:
        raise NotImplementedError
