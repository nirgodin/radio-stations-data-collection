from datetime import datetime
from typing import List

from aiohttp import ClientSession
from billboard import ChartData
from bs4 import BeautifulSoup
from genie_common.tools import AioPoolExecutor
from genie_common.utils import chain_lists
from genie_datastores.postgres.models import BillboardChart, ChartEntry, Chart

from data_collectors.consts.billboard_consts import (
    BILLBOARD_DATETIME_FORMAT,
)
from data_collectors.contract import IChartsDataCollector


class BillboardChartsCollector(IChartsDataCollector):
    def __init__(
        self, billboard_base_url: str, session: ClientSession, pool_executor: AioPoolExecutor = AioPoolExecutor()
    ):
        self._billboard_base_url = billboard_base_url
        self._session = session
        self._pool_executor = pool_executor

    async def collect(self, dates: List[datetime]) -> List[ChartEntry]:
        entries = await self._pool_executor.run(
            iterable=dates,
            func=self._collect_single_date_charts,
            expected_type=list,
        )
        return chain_lists(entries)

    async def _collect_single_date_charts(self, date: datetime) -> List[ChartEntry]:
        formatted_date = date.strftime(BILLBOARD_DATETIME_FORMAT)
        url = f"{self._billboard_base_url}/{BillboardChart.HOT_100.value}/{formatted_date}"

        async with self._session.get(url) as raw_response:
            chart_page_text = await raw_response.text()

        return self._create_chart_data_from_text(chart_page_text, formatted_date)

    def _create_chart_data_from_text(self, chart_page_text: str, formatted_date: str) -> List[ChartEntry]:
        chart_data = ChartData(name=BillboardChart.HOT_100.value, date=formatted_date, fetch=False)
        soup = BeautifulSoup(chart_page_text, "html.parser")
        chart_data._parsePage(soup)

        return self._to_chart_entries(chart_data, formatted_date)

    @staticmethod
    def _to_chart_entries(chart_data: ChartData, formatted_date: str) -> List[ChartEntry]:
        date = datetime.strptime(formatted_date, BILLBOARD_DATETIME_FORMAT)
        entries = []

        for billboard_entry in chart_data.entries:
            entry = ChartEntry(
                chart=Chart.BILLBOARD_HOT_100,
                date=date,
                position=billboard_entry.rank,
                key=f"{billboard_entry.artist} - {billboard_entry.title}",
            )
            entries.append(entry)

        return entries
