from copy import deepcopy
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from genie_common.tools import logger
from genie_datastores.postgres.models import ChartEntry, Chart
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.glglz_consts import FIRST_GLGLZ_CHART_DATE
from data_collectors.logic.collectors import (
    GlglzChartsDataCollector,
    ChartsTracksCollector,
)
from data_collectors.logic.inserters.postgres import (
    SpotifyInsertionsManager,
    ChartEntriesDatabaseInserter,
)
from data_collectors.logic.managers.charts.base_charts_manager import BaseChartsManager


class GlglzChartsManager(BaseChartsManager):
    def __init__(
        self,
        charts_data_collector: GlglzChartsDataCollector,
        charts_tracks_collector: ChartsTracksCollector,
        spotify_insertions_manager: SpotifyInsertionsManager,
        chart_entries_inserter: ChartEntriesDatabaseInserter,
        db_engine: AsyncEngine,
    ):
        super().__init__(
            charts_data_collector=charts_data_collector,
            charts_tracks_collector=charts_tracks_collector,
            spotify_insertions_manager=spotify_insertions_manager,
            chart_entries_inserter=chart_entries_inserter,
        )
        self._db_engine = db_engine

    async def _generate_data_collector_kwargs(
        self, dates: Optional[List[datetime]], limit: Optional[int]
    ) -> Dict[str, Any]:
        if dates is not None:
            return {"dates": dates}

        logger.info("No date was supplied. Querying database to find next missing charts dates")
        query = (
            select(ChartEntry.date)
            .where(ChartEntry.chart.in_([Chart.GLGLZ_WEEKLY_ISRAELI, Chart.GLGLZ_WEEKLY_INTERNATIONAL]))
            .order_by(ChartEntry.date.desc())
            .limit(1)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)
        last_chart_date = query_result.scalars().first()
        next_dates = self._build_next_dates(last_chart_date, limit) if last_chart_date else [FIRST_GLGLZ_CHART_DATE]

        return {"dates": next_dates}

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
