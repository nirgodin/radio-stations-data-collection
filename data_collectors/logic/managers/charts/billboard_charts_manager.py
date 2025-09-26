from datetime import timedelta, datetime
from typing import Dict, List

from genie_datastores.postgres.models import ChartEntry, Chart
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select, func

from data_collectors.consts.billboard_consts import FIRST_BILLBOARD_CHART_DATE
from data_collectors.logic.managers.charts.base_charts_manager import BaseChartsManager


class BillboardChartsManager(BaseChartsManager):
    async def _generate_data_collector_kwargs(self) -> Dict[str, List[datetime]]:
        query = select(func.max(ChartEntry.date)).where(ChartEntry.chart == Chart.BILLBOARD_HOT_100)
        query_result = await execute_query(engine=self._db_engine, query=query)
        last_chart_date = query_result.scalars().first()

        if last_chart_date:
            dates = [last_chart_date + timedelta(days=7)]
        else:
            dates = [FIRST_BILLBOARD_CHART_DATE]

        return {"dates": dates}
