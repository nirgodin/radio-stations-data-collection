import re
from datetime import datetime
from typing import List, Dict

from aiohttp import ClientSession
from genie_common.tools import AioPoolExecutor
from genie_common.utils import chain_lists
from genie_datastores.postgres.models import ChartEntry, Chart

from data_collectors.consts.billboard_consts import (
    BILLBOARD_DATETIME_FORMAT,
    BILLBOARD_HOT_100_CHART_NAME,
    SONG_LIST_ITEM_WEB_ELEMENT,
)
from data_collectors.contract import IChartsDataCollector
from data_collectors.logic.models import HTMLElement
from data_collectors.tools import WebElementsExtractor


class BillboardChartsCollector(IChartsDataCollector):
    def __init__(
        self,
        billboard_base_url: str,
        session: ClientSession,
        pool_executor: AioPoolExecutor = AioPoolExecutor(),
        web_elements_extractor: WebElementsExtractor = WebElementsExtractor(),
    ):
        self._billboard_base_url = billboard_base_url
        self._session = session
        self._pool_executor = pool_executor
        self._web_elements_extractor = web_elements_extractor

    async def collect(self, dates: List[datetime]) -> List[ChartEntry]:
        entries = await self._pool_executor.run(
            iterable=dates,
            func=self._collect_single_date_charts,
            expected_type=list,
        )
        return chain_lists(entries)

    async def _collect_single_date_charts(self, date: datetime) -> List[ChartEntry]:
        formatted_date = date.strftime(BILLBOARD_DATETIME_FORMAT)
        url = f"{self._billboard_base_url}/{BILLBOARD_HOT_100_CHART_NAME}/{formatted_date}"

        async with self._session.get(url) as raw_response:
            chart_page_text = await raw_response.text()

        return self._create_chart_data_from_text(chart_page_text, formatted_date)

    def _create_chart_data_from_text(self, chart_page_text: str, formatted_date: str) -> List[ChartEntry]:
        items = self._web_elements_extractor.extract(chart_page_text, SONG_LIST_ITEM_WEB_ELEMENT)
        date = datetime.strptime(formatted_date, BILLBOARD_DATETIME_FORMAT)
        entries = []

        for i, item in enumerate(items):
            entry = ChartEntry(
                chart=Chart.BILLBOARD_HOT_100,
                date=date,
                position=i + 1,
                key=self._to_key(item),
            )
            entries.append(entry)

        return entries

    @staticmethod
    def _to_key(item: Dict[str, str]) -> str:
        item_text = item[HTMLElement.A.value]
        collapsed_spaces_text = re.sub(r"[\t ]+", " ", item_text)
        collapsed_newlines_text = re.sub(r"\n\s*\n+", "\n", collapsed_spaces_text)
        stripped_text = collapsed_newlines_text.strip()
        artist, song = [e.strip() for e in stripped_text.split("\n")]

        return f"{artist} - {song}"
