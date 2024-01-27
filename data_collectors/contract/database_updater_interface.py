from abc import ABC
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine


class IDatabaseUpdater(ABC):
    def __init__(self, db_engine: AsyncEngine):
        self._db_engine = db_engine

    async def update(self, *args, **kwargs) -> Any:
        raise NotImplementedError
