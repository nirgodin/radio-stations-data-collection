from datetime import datetime
from typing import List

from aiohttp import ClientSession
from billboard import ChartData
from bs4 import BeautifulSoup
from genie_datastores.postgres.models import BillboardChart

from data_collectors.consts.billboard_consts import BILLBOARD_DATETIME_FORMAT, BILLBOARD_DAILY_CHARTS_URL_FORMAT
from data_collectors.contract.collector_interface import ICollector
from genie_common.tools import AioPoolExecutor


class BillboardChartsCollector(ICollector):
    def __init__(self, session: ClientSession, pool_executor: AioPoolExecutor = AioPoolExecutor()):
        self._session = session
        self._pool_executor = pool_executor

    async def collect(self, dates: List[datetime]) -> List[ChartData]:
        return await self._pool_executor.run(
            iterable=dates,
            func=self._collect_single_date_charts,
            expected_type=ChartData
        )

    async def _collect_single_date_charts(self, date: datetime) -> ChartData:
        formatted_date = date.strftime(BILLBOARD_DATETIME_FORMAT)
        url = BILLBOARD_DAILY_CHARTS_URL_FORMAT.format(name=BillboardChart.HOT_100.value, date=formatted_date)

        async with self._session.get(url) as raw_response:
            chart_page_text = await raw_response.text()

        return self._create_chart_data_from_text(chart_page_text, formatted_date)

    @staticmethod
    def _create_chart_data_from_text(chart_page_text: str, formatted_date: str) -> ChartData:
        chart_data = ChartData(name=BillboardChart.HOT_100.value, date=formatted_date, fetch=False)
        soup = BeautifulSoup(chart_page_text, "html.parser")
        chart_data._parsePage(soup)

        return chart_data
