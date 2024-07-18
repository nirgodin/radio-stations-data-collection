from typing import Optional

from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.logic.collectors import SpotifyArtistsAboutCollector
from data_collectors.contract import IManager


class SpotifyArtistsAboutManager(IManager):
    def __init__(self, db_engine: AsyncEngine, abouts_collector: SpotifyArtistsAboutCollector):
        self._abouts_collector = abouts_collector
        self._db_engine = db_engine

    async def run(self, limit: Optional[int]) -> None:
        pass
