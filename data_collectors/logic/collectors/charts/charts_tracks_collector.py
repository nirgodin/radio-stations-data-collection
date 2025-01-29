from typing import Any, List, Optional

from genie_common.tools import AioPoolExecutor, logger
from genie_datastores.postgres.models import ChartEntry, SpotifyTrack
from genie_datastores.postgres.operations import execute_query
from spotipyio import SpotifyClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.spotify_consts import TRACK
from data_collectors.contract import ICollector
from data_collectors.logic.collectors.charts.charts_searchers.base_chart_key_searcher import (
    BaseChartKeySearcher,
)
from data_collectors.logic.models.radio_chart_entry_details import (
    RadioChartEntryDetails,
)


class ChartsTracksCollector(ICollector):
    def __init__(
        self,
        pool_executor: AioPoolExecutor,
        db_engine: AsyncEngine,
        spotify_client: SpotifyClient,
        key_searcher: BaseChartKeySearcher,
    ):
        self._pool_executor = pool_executor
        self._db_engine = db_engine
        self._spotify_client = spotify_client
        self._key_searcher = key_searcher

    async def collect(
        self, charts_entries: List[ChartEntry]
    ) -> List[RadioChartEntryDetails]:
        logger.info(
            f"Starting to collect tracks for {len(charts_entries)} charts entries"
        )
        return await self._pool_executor.run(
            iterable=charts_entries,
            func=self._create_single_chart_entry_details,
            expected_type=RadioChartEntryDetails,
        )

    async def _create_single_chart_entry_details(
        self, chart_entry: ChartEntry
    ) -> RadioChartEntryDetails:
        if chart_entry.track_id is None:
            return await self._create_chart_entry_details_by_key(chart_entry)

        return await self._create_chart_entry_details_by_track_id(chart_entry)

    async def _create_chart_entry_details_by_key(
        self, chart_entry: ChartEntry
    ) -> RadioChartEntryDetails:
        track_id = await self._query_existing_tracks_by_key(chart_entry)

        if track_id is None:
            return await self._key_searcher.search(chart_entry)

        chart_entry.track_id = track_id
        return RadioChartEntryDetails(entry=chart_entry)

    async def _query_existing_tracks_by_key(
        self, chart_entry: ChartEntry
    ) -> Optional[str]:
        query = (
            select(ChartEntry.track_id)
            .where(ChartEntry.chart == chart_entry.chart)
            .where(ChartEntry.key == chart_entry.key)
            .limit(1)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().first()

    async def _create_chart_entry_details_by_track_id(
        self, chart_entry: ChartEntry
    ) -> RadioChartEntryDetails:
        is_existing_track_id = await self._is_existing_track_id(chart_entry.track_id)
        if is_existing_track_id:
            return RadioChartEntryDetails(entry=chart_entry)

        tracks = await self._spotify_client.tracks.info.run([chart_entry.track_id])

        if tracks:
            return RadioChartEntryDetails(entry=chart_entry, track={TRACK: tracks[0]})

        string_date = chart_entry.date.strftime("%d-%m-%Y")
        logger.info(
            f"Did not find any track matching `{chart_entry.track_id}` from date `{string_date}`. Ignoring"
        )

    async def _is_existing_track_id(self, track_id: str) -> bool:
        query = select(SpotifyTrack.id).where(SpotifyTrack.id == track_id).limit(1)
        query_result = await execute_query(engine=self._db_engine, query=query)
        database_id = query_result.scalars().first()

        return database_id is not None
