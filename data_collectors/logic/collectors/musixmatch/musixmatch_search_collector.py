from typing import List

from data_collectors.logic.collectors.musixmatch.base_musixmatch_collector import BaseMusixmatchCollector
from data_collectors.logic.models.musixmatch_query import MusixmatchQuery


class MusixmatchSearchCollector(BaseMusixmatchCollector):
    async def collect(self, queries: List[MusixmatchQuery]) -> List[dict]:
        return await self._pool_executor.run(
            iterable=queries,
            func=self._search_single_track,
            expected_type=dict
        )

    async def _search_single_track(self, query: MusixmatchQuery) -> dict:
        return await self._get(params=query.to_query_params())

    @property
    def _route(self) -> str:
        return "track.search"
