from datetime import datetime
from typing import Dict, Optional, List

from genie_common.tools import logger
from genie_datastores.postgres.models import ChartEntry, Chart
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.eurovision_consts import FIRST_EUROVISION_YEAR
from data_collectors.logic.collectors import (
    EurovisionChartsDataCollector,
    ChartsTracksCollector,
)
from data_collectors.logic.inserters.postgres import (
    SpotifyInsertionsManager,
    ChartEntriesDatabaseInserter,
)
from data_collectors.logic.managers.charts.base_charts_manager import BaseChartsManager


class EurovisionChartsManager(BaseChartsManager):
    def __init__(
        self,
        charts_data_collector: EurovisionChartsDataCollector,
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
        self, years: Optional[List[int]] = None, limit: Optional[int] = None
    ) -> Dict[str, List[int]]:
        if years is not None:
            return {"years": years}

        if limit is None:
            limit = 1

        last_chart_year = await self._query_last_db_eurovision_year()
        next_chart_year = self._get_next_chart_year(last_chart_year)
        years = range(next_chart_year, next_chart_year + limit)
        now = datetime.now()
        valid_years = [year for year in years if self._is_relevant_year(year, now)]

        return {"years": valid_years}

    async def _query_last_db_eurovision_year(self) -> Optional[int]:
        query = (
            select(ChartEntry.date)
            .where(ChartEntry.chart == Chart.EUROVISION)
            .order_by(ChartEntry.date.desc())
            .limit(1)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)
        last_chart_date = query_result.scalars().first()

        if last_chart_date is not None:
            return last_chart_date.year

    @staticmethod
    def _get_next_chart_year(last_chart_year: Optional[int]) -> int:
        if last_chart_year is None:
            return FIRST_EUROVISION_YEAR

        return last_chart_year + 1

    @staticmethod
    def _is_relevant_year(year: int, now: datetime) -> bool:
        if now.year - year > 1:
            return True

        if now.year - year <= 1:
            if now > datetime(year, 6, 1):
                return True

        logger.warn(
            f"Eurovision charts entries of year `{year}` are not available yet. Skipping"
        )
        return False
