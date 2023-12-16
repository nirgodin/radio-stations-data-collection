from datetime import datetime
from typing import List

from aiohttp import ClientSession
from billboard import ChartData
from genie_datastores.postgres.models import ChartEntryData
from spotipyio.logic.collectors.search_collectors.search_item import SearchItem
from spotipyio.logic.collectors.search_collectors.spotify_search_type import SpotifySearchType
from spotipyio.logic.spotify_client import SpotifyClient
from spotipyio.utils.spotify_utils import extract_first_search_result

from data_collectors.consts.billboard_consts import BILLBOARD_DATETIME_FORMAT
from data_collectors.consts.spotify_consts import TRACK
from data_collectors.contract.collector_interface import ICollector
from data_collectors.tools import AioPoolExecutor


class BillboardTracksCollector(ICollector):
    def __init__(self,
                 session: ClientSession,
                 spotify_client: SpotifyClient,
                 pool_executor: AioPoolExecutor = AioPoolExecutor()):
        self._session = session
        self._spotify_client = spotify_client
        self._pool_executor = pool_executor

    async def collect(self, charts: List[ChartData]) -> List[ChartEntryData]:
        chart_entries = self._get_flattened_chart_entries(charts)
        return await self._pool_executor.run(
            iterable=chart_entries,
            func=self._collect_single,
            expected_type=ChartEntryData
        )

    async def _collect_single(self, entry_data: ChartEntryData) -> ChartEntryData:
        search_item = SearchItem(
            search_types=[SpotifySearchType.TRACK],
            artist=entry_data.entry.artist,
            track=entry_data.entry.title
        )
        search_result = await self._spotify_client.search.run_single(search_item)
        raw_track = extract_first_search_result(search_result)
        entry_data.track = {TRACK: raw_track} if raw_track else None

        return entry_data

    @staticmethod
    def _get_flattened_chart_entries(charts: List[ChartData]) -> List[ChartEntryData]:
        entries = []

        for chart in charts:
            for entry in chart.entries:
                entry_data = ChartEntryData(
                    entry=entry,
                    date=datetime.strptime(chart.date, BILLBOARD_DATETIME_FORMAT),
                    chart=chart.name
                )
                entries.append(entry_data)

        return entries
