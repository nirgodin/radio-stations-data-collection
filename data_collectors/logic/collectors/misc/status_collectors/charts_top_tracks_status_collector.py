from collections import defaultdict
from datetime import timedelta, datetime
from typing import List, Dict

import pandas as pd
from genie_common.tools import AioPoolExecutor
from genie_common.utils import safe_nested_get
from genie_datastores.postgres.models import (
    ChartEntry,
    Chart,
    SpotifyArtist,
    SpotifyTrack,
)
from genie_datastores.postgres.operations import execute_query, read_sql
from sqlalchemy import select, func
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.general_consts import DAY_LABEL
from data_collectors.contract import IStatusCollector
from data_collectors.logic.models import SummarySection


class ChartsTopTracksStatusCollector(IStatusCollector):
    def __init__(self, db_engine: AsyncEngine, pool_executor: AioPoolExecutor = AioPoolExecutor()):
        super().__init__(db_engine)
        self._pool_executor = pool_executor

    async def collect(self, lookback_period: timedelta) -> List[SummarySection]:
        start_date = datetime.now() - lookback_period
        charts_dates = await self._query_latest_charts_dates(start_date)

        return await self._pool_executor.run(
            iterable=charts_dates, func=self._query_top_chart_entries, expected_type=SummarySection
        )

    async def _query_latest_charts_dates(self, start_date: datetime) -> List[Row]:
        query = (
            select(ChartEntry.chart, func.max(ChartEntry.date))
            .where(ChartEntry.creation_date >= start_date)
            .group_by(ChartEntry.chart)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.all()

    async def _query_top_chart_entries(self, row: Row) -> SummarySection:
        query = (
            select(SpotifyArtist.name.label("artist_name"), SpotifyTrack.name, ChartEntry.position)
            .where(ChartEntry.track_id == SpotifyTrack.id)
            .where(SpotifyTrack.artist_id == SpotifyArtist.id)
            .where(ChartEntry.track_id.isnot(None))
            .where(ChartEntry.chart == row.chart)
            .where(ChartEntry.date == row.max)
            .order_by(ChartEntry.position.asc())
            .limit(10)
        )
        data = await read_sql(engine=self._db_engine, query=query)
        title = f"{row.chart.name} | {row.max.strftime('%d/%m/%Y')}"

        return SummarySection(title=title, data=data)

    @staticmethod
    def _group_records_by_chart(
        rows: List[Row],
    ) -> Dict[Chart, List[Dict[str, str]]]:
        grouped_records = defaultdict(list)

        for row in rows:
            record_day: datetime = getattr(row, DAY_LABEL)
            record = {"date": record_day.strftime("%d/%m/%Y"), "count": row.count}
            grouped_records[row.chart].append(record)

        return grouped_records

    def _summarize(
        self,
        count_records: Dict[str, Dict[datetime, List[Row]]],
        top_entries_records: Dict[str, Dict[datetime, List[Row]]],
    ) -> str:
        summaries = []

        for chart, records in count_records.items():
            chart_summary = f"{chart}:"

            for date, rows in records.items():
                date_summary = self._summarize_single_chart_day(chart, date, top_entries_records, rows)
                chart_summary += f"\n{date_summary}"

            summaries.append(chart_summary)

        return "\n\n".join(summaries)

    @staticmethod
    def _summarize_single_chart_day(
        chart: str,
        date: datetime,
        top_entries_records: Dict[str, Dict[datetime, List[Row]]],
        count_records: List[Row],
    ) -> str:
        date_summary = f"{date.strftime('%d/%m/%Y')}:"
        date_count = str(count_records[0].count) if count_records else "Unknown"
        date_summary += f"\nTotal records: {date_count}"
        date_summary += "\nTop entries:"
        top_entries = safe_nested_get(top_entries_records, [chart, date], default=[])

        for entry in sorted(top_entries, key=lambda row: row.position):
            date_summary += f"\n{entry.position}. {entry.artist_name} - {entry.name}"

        return date_summary

    @staticmethod
    def _to_summary_sections(grouped_records: Dict[Chart, List[dict]]) -> List[SummarySection]:
        summary_sections = []

        for chart, records in grouped_records.items():
            data = pd.DataFrame.from_records(records)
            section = SummarySection(title=chart.name, data=data)
            summary_sections.append(section)

        return sorted(summary_sections, key=lambda x: x.title)

    @property
    def name(self) -> str:
        return "Charts Top Tracks"
