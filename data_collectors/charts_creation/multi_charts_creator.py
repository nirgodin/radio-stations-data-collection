from typing import Optional, List

from genie_common.tools import AioPoolExecutor
from genie_datastores.postgres.models import ChartEntry, Chart
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.charts_creation.charts_creator import ChartsCreator


class MultiChartsCreator:
    def __init__(self, db_engine: AsyncEngine, pool_executor: AioPoolExecutor, charts_creator: ChartsCreator):
        self._db_engine = db_engine
        self._pool_executor = pool_executor
        self._charts_creator = charts_creator

    async def create(self, limit: Optional[int]) -> None:
        charts_dates_tuples = await self._query_charts_and_dates(limit)
        await self._pool_executor.run(
            iterable=charts_dates_tuples,
            func=self._create_single_chart,
            expected_type=type(None),
        )

    async def _query_charts_and_dates(self, limit: Optional[int]) -> List[Row]:
        relevant_charts = [Chart.GLGLZ_WEEKLY_ISRAELI, Chart.GLGLZ_WEEKLY_INTERNATIONAL]
        query = (
            select(ChartEntry.chart, ChartEntry.date)
            .distinct()
            .where(ChartEntry.chart.in_(relevant_charts))
            .order_by(ChartEntry.date.asc())
            .limit(limit)
        )
        query_result = await execute_query(self._db_engine, query)

        return query_result.all()

    async def _create_single_chart(self, row: Row) -> None:
        await self._charts_creator.create(chart=row.chart, date=row.date)
