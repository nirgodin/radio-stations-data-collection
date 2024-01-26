from abc import ABC, abstractmethod
from typing import Any


class IExporter(ABC):
    @abstractmethod
    async def export(self, *args, **kwargs) -> Any:
        raise NotImplementedError
