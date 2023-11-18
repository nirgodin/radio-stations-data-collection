from abc import ABC, abstractmethod
from typing import Any


class ICollector(ABC):
    @abstractmethod
    async def collect(self, *args, **kwargs) -> Any:
        raise NotImplementedError
