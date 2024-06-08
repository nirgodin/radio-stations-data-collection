from abc import ABC, abstractmethod
from typing import Optional, List

from genie_datastores.postgres.models import DataSource
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import ICollector
from data_collectors.logic.models import ArtistExistingDetails


class BaseArtistsExistingDetailsCollector(ICollector, ABC):
    def __init__(self, db_engine: AsyncEngine):
        self._db_engine = db_engine

    @abstractmethod
    async def collect(self, limit: Optional[int]) -> List[ArtistExistingDetails]:
        raise NotImplementedError

    @property
    @abstractmethod
    def data_source(self) -> DataSource:
        raise NotImplementedError
