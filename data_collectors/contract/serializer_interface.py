from abc import ABC, abstractmethod
from typing import Any


class ISerializer(ABC):
    @abstractmethod
    def serialize(self, *args, **kwargs) -> Any:
        raise NotImplementedError
