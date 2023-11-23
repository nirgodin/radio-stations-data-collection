from abc import ABC, abstractmethod


class IManager(ABC):
    @abstractmethod
    async def run(self, *args, **kwargs):
        raise NotImplementedError
