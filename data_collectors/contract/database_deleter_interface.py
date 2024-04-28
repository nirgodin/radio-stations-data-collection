from abc import ABC


class IDatabaseDeleter(ABC):
    async def delete(self, *args, **kwargs) -> None:
        raise NotImplementedError
