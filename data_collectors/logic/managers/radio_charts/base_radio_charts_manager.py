from abc import ABC, abstractmethod
from typing import Any, List, Dict

from genie_common.tools import logger

from data_collectors.contract import IChartsDataCollector, IManager
from data_collectors.logic.collectors import RadioChartsTracksCollector
from data_collectors.logic.inserters.postgres import SpotifyInsertionsManager, ChartEntriesDatabaseInserter
from data_collectors.logic.models import RadioChartEntryDetails


class BaseRadioChartsManager(IManager, ABC):
    def __init__(self,
                 charts_data_collector: IChartsDataCollector,
                 charts_tracks_collector: RadioChartsTracksCollector,
                 spotify_insertions_manager: SpotifyInsertionsManager,
                 chart_entries_inserter: ChartEntriesDatabaseInserter):
        self._charts_data_collector = charts_data_collector
        self._charts_tracks_collector = charts_tracks_collector
        self._spotify_insertions_manager = spotify_insertions_manager
        self._chart_entries_inserter = chart_entries_inserter

    async def run(self, *args, **kwargs) -> None:
        logger.info(f"Starting to run `{self.__class__.__name__}` charts manager")
        data_collector_kwargs: Dict[str, Any] = await self._generate_data_collector_order_args(*args, **kwargs)
        charts_entries = await self._charts_data_collector.collect(**data_collector_kwargs)

        if charts_entries:
            charts_entries_details = await self._charts_tracks_collector.collect(charts_entries)
            await self._insert_records(charts_entries_details)

        else:
            logger.warn("Did not find any chart entry. Aborting")

    @abstractmethod
    async def _generate_data_collector_order_args(self, *args, **kwargs) -> Dict[str, Any]:
        raise NotImplementedError

    async def _insert_records(self, charts_entries_details: List[RadioChartEntryDetails]) -> None:
        spotify_tracks = [detail.track for detail in charts_entries_details if detail.track is not None]

        if spotify_tracks:
            logger.info("Starting to insert spotify tracks to all relevant tables")
            await self._spotify_insertions_manager.insert(spotify_tracks)

        records = [detail.entry for detail in charts_entries_details if detail.entry.track_id is not None]
        logger.info("Starting to insert chart entries")
        await self._chart_entries_inserter.insert(records)
