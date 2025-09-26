from abc import ABC, abstractmethod
from datetime import timedelta
from typing import List

from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract.collector_interface import ICollector
from data_collectors.logic.models.summary_section import SummarySection


class IStatusCollector(ICollector, ABC):
    def __init__(self, db_engine: AsyncEngine):
        self._db_engine = db_engine

    async def collect(self, lookback_period: timedelta) -> List[SummarySection]:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError
