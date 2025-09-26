from datetime import datetime, timedelta
from typing import Dict, List, Optional

from genie_common.utils import get_last_month_day
from genie_datastores.postgres.models import ChartEntry, Chart
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select

from data_collectors.consts.every_hit_consts import LAST_DATE_WITHOUT_EVERY_HIT_CHART
from data_collectors.logic.managers.charts.base_charts_manager import BaseChartsManager
from data_collectors.logic.models import DateRange


class EveryHitChartsManager(BaseChartsManager):
    async def _generate_data_collector_kwargs(
        self, date_ranges: Optional[List[DateRange]], limit: Optional[int]
    ) -> Dict[str, List[DateRange]]:
        if date_ranges is None:
            last_chart_date = await self._query_last_chart_date() or LAST_DATE_WITHOUT_EVERY_HIT_CHART
            date_ranges = self._generate_date_ranges(last_chart_date, limit)

        return {"date_ranges": date_ranges}

    async def _query_last_chart_date(self) -> Optional[datetime]:
        query = (
            select(ChartEntry.date)
            .where(ChartEntry.chart == Chart.UK_EVERY_HIT)
            .order_by(ChartEntry.date.desc())
            .limit(1)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().first()

    def _generate_date_ranges(self, last_chart_date: datetime, limit: int) -> List[DateRange]:
        date_ranges = [self._build_single_date_range(last_chart_date)]

        while len(date_ranges) < limit:
            last_date_range = date_ranges[-1]
            next_date_range = self._build_single_date_range(last_date_range.start_date)
            date_ranges.append(next_date_range)

        return date_ranges

    @staticmethod
    def _build_single_date_range(last_month_date: datetime) -> DateRange:
        raw_start_date = last_month_date + timedelta(days=31)
        start_date = datetime(raw_start_date.year, raw_start_date.month, 1)
        last_month_day = get_last_month_day(start_date)
        end_date = datetime(start_date.year, start_date.month, last_month_day)

        return DateRange(start_date=start_date, end_date=end_date)
