from datetime import datetime
from functools import partial
from typing import List, Optional

from genie_common.tools import AioPoolExecutor, logger
from genie_datastores.postgres.models import ChartEntry, Chart
from genie_datastores.postgres.operations import execute_query
from pandas import Series
from spotipyio import SpotifyClient, SearchItemMetadata, SearchItem, SpotifySearchType
from spotipyio.utils import extract_first_search_result
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.radio_charts_consts import ARTIST_COLUMN_NAME, SONG_COLUMN_NAME, POSITION_COLUMN_NAME
from data_collectors.consts.spotify_consts import ID, TRACK
from data_collectors.contract import ICollector
from data_collectors.logic.models import RadioChartData, RadioChartEntryDetails


class RadioChartsTracksCollector(ICollector):
    def __init__(self, pool_executor: AioPoolExecutor, spotify_client: SpotifyClient, db_engine: AsyncEngine):
        self._pool_executor = pool_executor
        self._spotify_client = spotify_client
        self._db_engine = db_engine

    async def collect(self, charts_data: List[RadioChartData], chart: Chart) -> List[RadioChartEntryDetails]:
        logger.info(f"Starting to collect tracks for {len(charts_data)} charts")
        charts_entries = []

        for chart_data in charts_data:
            func = partial(
                self._convert_row_to_chart_entry,
                chart,
                chart_data.date,
                chart_data.original_file_name
            )
            chart_entries = await self._pool_executor.run(
                iterable=[row for _, row in chart_data.data.iterrows()],
                func=func,
                expected_type=RadioChartEntryDetails
            )
            charts_entries.extend(chart_entries)

        return charts_entries

    async def _convert_row_to_chart_entry(self,
                                          chart: Chart,
                                          date: datetime,
                                          original_file_name: str,
                                          row: Series) -> Optional[RadioChartEntryDetails]:
        entry_key = f"{row[ARTIST_COLUMN_NAME]} - {row[SONG_COLUMN_NAME]}"
        track_id = await self._query_existing_tracks_by_key(entry_key, chart)

        if track_id is None:
            return await self._create_non_existing_chart_entry(
                entry_key=entry_key,
                row=row,
                date=date,
                chart=chart,
                original_file_name=original_file_name
            )

        return RadioChartEntryDetails(
            entry=ChartEntry(
                track_id=track_id,
                chart=chart,
                date=date,
                key=entry_key,
                position=row[POSITION_COLUMN_NAME],
                comment=original_file_name
            )
        )

    async def _query_existing_tracks_by_key(self, entry_key: str, chart: Chart) -> Optional[str]:
        query = (
            select(ChartEntry.track_id)
            .where(ChartEntry.chart == chart)
            .where(ChartEntry.key == entry_key)
            .limit(1)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().first()

    async def _create_non_existing_chart_entry(self,
                                               entry_key: str,
                                               row: Series,
                                               date: datetime,
                                               chart: Chart,
                                               original_file_name: str) -> Optional[RadioChartEntryDetails]:
        search_item = SearchItem(
            text=entry_key,
            metadata=SearchItemMetadata(
                search_types=[SpotifySearchType.TRACK],
                quote=False
            ),
        )
        search_result = await self._spotify_client.search.run_single(search_item)
        track = extract_first_search_result(search_result)

        if track is None:
            self._log_track_not_found(entry_key, date)
        else:
            return RadioChartEntryDetails(
                entry=ChartEntry(
                    track_id=track[ID],
                    chart=chart,
                    date=date,
                    key=entry_key,
                    position=row[POSITION_COLUMN_NAME],
                    comment=original_file_name
                ),
                track={TRACK: track}
            )

    @staticmethod
    def _log_track_not_found(entry_key: str, date: datetime) -> None:
        stringified_date = date.strftime("%d-%m-%Y")
        logger.info(f"Did not find any track that matches `{entry_key}` from date `{stringified_date}`. Ignoring")
