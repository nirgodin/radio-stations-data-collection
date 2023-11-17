from datetime import datetime
from functools import partial
from typing import List

from aiohttp import ClientSession
from asyncio_pool import AioPool
from billboard import ChartData
from bs4 import BeautifulSoup
from postgres_client.models.enum.billboard_chart import BillboardChart
from tqdm import tqdm

from data_collectors.consts.billboard_consts import BILLBOARD_DATETIME_FORMAT, BILLBOARD_DAILY_CHARTS_URL_FORMAT


class BillboardChartsCollector:
    def __init__(self, session: ClientSession):
        self._session = session

    async def collect(self, dates: List[datetime]):
        pool = AioPool(5)

        with tqdm(total=len(dates)) as progress_bar:
            func = partial(self._collect_single_date_charts, progress_bar)
            return await pool.map(func, dates)

    async def _collect_single_date_charts(self, progress_bar: tqdm, date: datetime) -> ChartData:
        progress_bar.update(1)
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
