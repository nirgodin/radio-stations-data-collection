from abc import ABC, abstractmethod
from typing import Any, Tuple, List

from genie_common.tools import logger
from genie_datastores.postgres.operations import insert_records
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.logic.collectors import RadioChartsTracksCollector
from data_collectors.contract import IChartsDataCollector, IManager
from data_collectors.logic.inserters.postgres import SpotifyInsertionsManager
from data_collectors.logic.models import RadioChartEntryDetails


class BaseRadioChartsManager(IManager, ABC):
    def __init__(self,
                 charts_data_collector: IChartsDataCollector,
                 charts_tracks_collector: RadioChartsTracksCollector,
                 spotify_insertions_manager: SpotifyInsertionsManager,
                 db_engine: AsyncEngine):
        self._charts_data_collector = charts_data_collector
        self._charts_tracks_collector = charts_tracks_collector
        self._spotify_insertions_manager = spotify_insertions_manager
        self._db_engine = db_engine

    async def run(self, *args, **kwargs) -> None:
        logger.info(f"Starting to run `{self.__class__.__name__}` charts manager")
        data_collector_args = await self._generate_data_collector_order_args(*args, **kwargs)
        charts_entries = await self._charts_data_collector.collect(*data_collector_args)
        charts_entries_details = await self._charts_tracks_collector.collect(charts_entries)
        await self._insert_records(charts_entries_details)

    @abstractmethod
    async def _generate_data_collector_order_args(self, *args, **kwargs) -> Tuple[Any]:
        raise NotImplementedError

    async def _insert_records(self, charts_entries_details: List[RadioChartEntryDetails]) -> None:
        spotify_tracks = [detail.track for detail in charts_entries_details if detail.track is not None]

        if spotify_tracks:
            logger.info("Starting to insert spotify tracks to all relevant tables")
            await self._spotify_insertions_manager.insert(spotify_tracks)

        records = [detail.entry for detail in charts_entries_details if detail.entry.track_id is not None]
        logger.info("Starting to insert chart entries")
        await insert_records(engine=self._db_engine, records=records)
