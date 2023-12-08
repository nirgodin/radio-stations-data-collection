from abc import ABC, abstractmethod


class IDatabaseInserter(ABC):
    @abstractmethod
    async def insert(self, *args, **kwargs):
        raise NotImplementedError
