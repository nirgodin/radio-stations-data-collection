from abc import ABC, abstractmethod
from datetime import timedelta

from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract.collector_interface import ICollector


class IStatusCollector(ICollector, ABC):
    def __init__(self, db_engine: AsyncEngine):
        self._db_engine = db_engine

    async def collect(self, lookback_period: timedelta) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError
