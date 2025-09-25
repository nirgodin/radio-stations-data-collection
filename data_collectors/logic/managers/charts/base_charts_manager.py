import json
from abc import ABC, abstractmethod
from typing import Any, List, Dict

from genie_common.tools import logger
from genie_common.utils import sort_dict_by_key
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import IChartsDataCollector, IManager
from data_collectors.logic.collectors import ChartsTracksCollector
from data_collectors.logic.inserters.postgres import (
    SpotifyInsertionsManager,
    ChartEntriesDatabaseInserter,
)
from data_collectors.logic.models import RadioChartEntryDetails


class BaseChartsManager(IManager, ABC):
    def __init__(
        self,
        charts_data_collector: IChartsDataCollector,
        charts_tracks_collector: ChartsTracksCollector,
        spotify_insertions_manager: SpotifyInsertionsManager,
        chart_entries_inserter: ChartEntriesDatabaseInserter,
        db_engine: AsyncEngine,
    ):
        self._charts_data_collector = charts_data_collector
        self._charts_tracks_collector = charts_tracks_collector
        self._spotify_insertions_manager = spotify_insertions_manager
        self._chart_entries_inserter = chart_entries_inserter
        self._db_engine = db_engine

    async def run(self, *args, **kwargs) -> None:
        logger.info(f"Starting to run `{self.__class__.__name__}` charts manager")
        data_collector_kwargs: Dict[str, Any] = await self._generate_data_collector_kwargs(*args, **kwargs)
        charts_entries = await self._charts_data_collector.collect(**data_collector_kwargs)

        if charts_entries:
            charts_entries_details = await self._charts_tracks_collector.collect(charts_entries)
            unique_entries_details = self._filter_unique_entries(charts_entries_details)
            await self._insert_records(unique_entries_details)

        else:
            logger.warn("Did not find any chart entry. Aborting")

    @abstractmethod
    async def _generate_data_collector_kwargs(self, *args, **kwargs) -> Dict[str, Any]:
        raise NotImplementedError

    async def _insert_records(self, charts_entries_details: List[RadioChartEntryDetails]) -> None:
        spotify_tracks = [detail.track for detail in charts_entries_details if detail.track is not None]

        if spotify_tracks:
            logger.info("Starting to insert spotify tracks to all relevant tables")
            await self._spotify_insertions_manager.insert(spotify_tracks)

        records = [detail.entry for detail in charts_entries_details]
        logger.info("Starting to insert chart entries")
        await self._chart_entries_inserter.insert(records)

    def _filter_unique_entries(
        self, charts_entries_details: List[RadioChartEntryDetails]
    ) -> List[RadioChartEntryDetails]:
        filtered_entries = []
        metadata_entries_map = {}

        for entry_details in charts_entries_details:
            if entry_details.entry.entry_metadata is None:
                filtered_entries.append(entry_details)
            else:
                serialized_metadata = json.dumps(sort_dict_by_key(entry_details.entry.entry_metadata))

                if serialized_metadata in metadata_entries_map.keys():
                    metadata_entries_map[serialized_metadata].append(entry_details)
                else:
                    metadata_entries_map[serialized_metadata] = [entry_details]

        return filtered_entries + self._extract_unique_values_from_metadata_entries_map(metadata_entries_map)

    @staticmethod
    def _extract_unique_values_from_metadata_entries_map(
        metadata_entries_map: Dict[str, List[RadioChartEntryDetails]]
    ) -> List[RadioChartEntryDetails]:
        unique_entries = []

        for entries_details in metadata_entries_map.values():
            entries_with_track = [detail for detail in entries_details if detail.track is not None]

            if entries_with_track:
                unique_entries.append(entries_with_track[0])
            else:
                unique_entries.append(entries_details[0])

        return unique_entries
