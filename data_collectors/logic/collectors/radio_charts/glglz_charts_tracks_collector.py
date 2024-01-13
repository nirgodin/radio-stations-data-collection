from typing import Any, List, Optional

from genie_common.tools import AioPoolExecutor, logger
from genie_datastores.postgres.models import ChartEntry
from genie_datastores.postgres.operations import execute_query
from spotipyio import SpotifyClient, SearchItem, SearchItemMetadata, SpotifySearchType
from spotipyio.utils import extract_first_search_result
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.spotify_consts import ID, TRACK
from data_collectors.contract import ICollector
from data_collectors.logic.models.radio_chart_entry_details import RadioChartEntryDetails


class GlglzChartsTracksCollector(ICollector):
    def __init__(self, pool_executor: AioPoolExecutor, db_engine: AsyncEngine, spotify_client: SpotifyClient):
        self._pool_executor = pool_executor
        self._db_engine = db_engine
        self._spotify_client = spotify_client

    async def collect(self, charts_entries: List[ChartEntry]) -> Any:
        logger.info(f"Starting to collect tracks for {len(charts_entries)} charts entries")
        return await self._pool_executor.run(
            iterable=charts_entries,
            func=self._create_single_chart_entry_details,
            expected_type=RadioChartEntryDetails
        )

    async def _create_single_chart_entry_details(self, chart_entry: ChartEntry) -> RadioChartEntryDetails:
        track_id = await self._query_existing_tracks_by_key(chart_entry)

        if track_id is None:
            return await self._create_non_existing_chart_entry(chart_entry)

        return RadioChartEntryDetails(entry=chart_entry)

    async def _query_existing_tracks_by_key(self, chart_entry: ChartEntry) -> Optional[str]:
        query = (
            select(ChartEntry.track_id)
            .where(ChartEntry.chart == chart_entry.chart)
            .where(ChartEntry.key == chart_entry.key)
            .limit(1)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().first()

    async def _create_non_existing_chart_entry(self, chart_entry: ChartEntry) -> Optional[RadioChartEntryDetails]:
        search_item = SearchItem(
            text=chart_entry.key,
            metadata=SearchItemMetadata(
                search_types=[SpotifySearchType.TRACK],
                quote=False
            )
        )
        search_result = await self._spotify_client.search.run_single(search_item)
        track = extract_first_search_result(search_result)

        if track is None:
            self._log_track_not_found(chart_entry)
        else:
            chart_entry.track_id = track[ID]
            return RadioChartEntryDetails(
                entry=chart_entry,
                track={TRACK: track}
            )

    @staticmethod
    def _log_track_not_found(chart_entry: ChartEntry) -> None:
        stringified_date = chart_entry.date.strftime("%d-%m-%Y")
        logger.info(f"Did not find any track that matches `{chart_entry.key}` from date `{stringified_date}`. Ignoring")
