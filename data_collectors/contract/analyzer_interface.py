from abc import ABC, abstractmethod
from typing import Any


class IAnalyzer(ABC):
    @abstractmethod
    def analyze(self, *args, **kwargs) -> Any:
        raise NotImplementedError
