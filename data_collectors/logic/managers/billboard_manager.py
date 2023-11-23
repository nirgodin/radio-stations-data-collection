from datetime import datetime, timedelta
from typing import List

from postgres_client import BillboardChartEntry, execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.billboard_consts import FIRST_BILLBOARD_CHART_DATE
from data_collectors.contract import IManager
from data_collectors.logic.collectors.billboard import *
from data_collectors.logic.inserters.billboard import *
from data_collectors.logic.inserters import SpotifyInsertionsManager
from data_collectors.logic.updaters import BillboardTracksDatabaseUpdater


class BillboardManager(IManager):
    def __init__(self,
                 db_engine: AsyncEngine,
                 charts_collector: BillboardChartsCollector,
                 tracks_collector: BillboardTracksCollector,
                 spotify_insertions_manager: SpotifyInsertionsManager,
                 tracks_inserter: BillboardTracksDatabaseInserter,
                 charts_inserter: BillboardChartsDatabaseInserter,
                 tracks_updater: BillboardTracksDatabaseUpdater):
        self._db_engine = db_engine
        self._charts_collector = charts_collector
        self._tracks_collector = tracks_collector
        self._spotify_insertions_manager = spotify_insertions_manager
        self._tracks_inserter = tracks_inserter
        self._charts_inserter = charts_inserter
        self._tracks_updater = tracks_updater

    async def run(self, dates: List[datetime] = None) -> None:
        if dates is None:
            dates = await self._retrieve_next_missing_date()

        charts = await self._charts_collector.collect(dates)
        entries = await self._tracks_collector.collect(charts)
        tracks = [entry.track for entry in entries if isinstance(entry.track, dict)]
        await self._spotify_insertions_manager.insert(tracks)
        await self._tracks_inserter.insert(entries)
        await self._charts_inserter.insert(entries)
        await self._tracks_updater.update(entries)

    async def _retrieve_next_missing_date(self) -> List[datetime]:
        query = (
            select(BillboardChartEntry.date)
            .order_by(BillboardChartEntry.date.desc())
            .limit(1)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)
        last_chart_date = query_result.scalars().first()

        if last_chart_date:
            return [last_chart_date + timedelta(days=7)]

        return [FIRST_BILLBOARD_CHART_DATE]
