from copy import deepcopy
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

import requests
from genie_common.tools import logger
from genie_datastores.postgres.models import ChartEntry, Chart
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.logic.collectors import GlglzChartsDataCollector, RadioChartsTracksCollector
from data_collectors.consts.glglz_consts import FIRST_GLGLZ_CHART_DATE, GLGLZ_DATETIME_FORMAT, \
    GLGLZ_WEEKLY_CHART_URL_FORMAT
from data_collectors.logic.inserters.postgres import SpotifyInsertionsManager, ChartEntriesDatabaseInserter
from data_collectors.logic.managers.radio_charts.base_radio_charts_manager import BaseRadioChartsManager


class GlglzChartsManager(BaseRadioChartsManager):
    def __init__(self,
                 charts_data_collector: GlglzChartsDataCollector,
                 charts_tracks_collector: RadioChartsTracksCollector,
                 spotify_insertions_manager: SpotifyInsertionsManager,
                 chart_entries_inserter: ChartEntriesDatabaseInserter,
                 db_engine: AsyncEngine):
        super().__init__(
            charts_data_collector=charts_data_collector,
            charts_tracks_collector=charts_tracks_collector,
            spotify_insertions_manager=spotify_insertions_manager,
            chart_entries_inserter=chart_entries_inserter
        )
        self._db_engine = db_engine

    async def _generate_data_collector_order_args(self,
                                                  dates: Optional[List[datetime]],
                                                  limit: Optional[int]) -> Dict[str, Any]:
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

    def _build_next_dates(self, last_chart_date: datetime, limit: Optional[int]) -> List[datetime]:
        if limit is None:
            limit = 1

        now = datetime.now()
        dates = []

        while len(dates) < limit and last_chart_date <= now:
            last_chart_date += timedelta(days=7)
            if self._is_valid_date(last_chart_date):
                dates.append(deepcopy(last_chart_date))

        return dates

    @staticmethod
    def _is_valid_date(date: datetime) -> bool:
        formatted_date = date.strftime(GLGLZ_DATETIME_FORMAT).strip()
        url = GLGLZ_WEEKLY_CHART_URL_FORMAT.format(date=formatted_date)
        response = requests.get(url)

        return response.ok
