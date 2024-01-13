from copy import deepcopy
from datetime import datetime, timedelta
from typing import Optional, List

from genie_common.tools import logger
from genie_datastores.postgres.models import ChartEntry, Chart
from genie_datastores.postgres.operations import execute_query, insert_records
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.logic.collectors import GlglzChartsDataCollector, GlglzChartsTracksCollector
from data_collectors.consts.glglz_consts import FIRST_GLGLZ_CHART_DATE
from data_collectors.contract import IManager
from data_collectors.logic.inserters.postgres import SpotifyInsertionsManager
from data_collectors.logic.models import RadioChartEntryDetails


class GlglzChartsManager(IManager):
    def __init__(self,
                 db_engine: AsyncEngine,
                 charts_data_collector: GlglzChartsDataCollector,
                 charts_tracks_collector: GlglzChartsTracksCollector,
                 spotify_insertions_manager: SpotifyInsertionsManager):
        self._db_engine = db_engine
        self._charts_data_collector = charts_data_collector
        self._charts_tracks_collector = charts_tracks_collector
        self._spotify_insertions_manager = spotify_insertions_manager

    async def run(self, dates: Optional[List[datetime]], limit: Optional[int]):
        logger.info(f"Starting to run glglz charts manager")
        if dates is None:
            dates = await self._retrieve_next_missing_dates(limit)

        charts_entries = await self._charts_data_collector.collect(dates)
        charts_entries_details = await self._charts_tracks_collector.collect(charts_entries)
        await self._insert_records(charts_entries_details)

    async def _retrieve_next_missing_dates(self, limit: Optional[int]) -> List[datetime]:
        logger.info("No date was supplied. Querying database to find next missing charts dates")
        query = (
            select(ChartEntry.date)
            .where(ChartEntry.chart.in_([Chart.GLGLZ_WEEKLY_ISRAELI, Chart.GLGLZ_WEEKLY_INTERNATIONAL]))
            .order_by(ChartEntry.date.desc())
            .limit(1)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)
        last_chart_date = query_result.scalars().first()

        if last_chart_date:
            return self._build_next_dates(last_chart_date, limit)

        return [FIRST_GLGLZ_CHART_DATE]

    @staticmethod
    def _build_next_dates(last_chart_date: datetime, limit: Optional[int]) -> List[datetime]:
        if limit is None:
            limit = 1

        now = datetime.now()
        dates = []

        while len(dates) < limit and last_chart_date <= now:
            last_chart_date += timedelta(days=7)
            dates.append(deepcopy(last_chart_date))

        return dates

    async def _insert_records(self, charts_entries_details: List[RadioChartEntryDetails]) -> None:
        spotify_tracks = [detail.track for detail in charts_entries_details if detail.track is not None]

        if spotify_tracks:
            logger.info("Starting to insert spotify tracks to all relevant tables")
            await self._spotify_insertions_manager.insert(spotify_tracks)

        records = [detail.entry for detail in charts_entries_details]
        logger.info("Starting to insert chart entries")
        await insert_records(engine=self._db_engine, records=records)
