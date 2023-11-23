from typing import List, Dict, Optional

from data_collectors.consts.shazam_consts import HITS, KEY
from data_collectors.consts.spotify_consts import TRACKS
from data_collectors.logic.collectors.shazam.base_shazam_collector import BaseShazamCollector
from data_collectors.logic.utils.dict_utils import merge_dicts, safe_nested_get
from data_collectors.logs import logger


class ShazamSearchCollector(BaseShazamCollector):
    async def collect(self, queries: List[str]) -> Dict[str, str]:
        n_queries = len(queries)
        logger.info(f"Starting to search shazam for {n_queries} queries")
        results = await self._pool_executor.run(
            iterable=queries,
            func=self._search_single_track
        )
        valid_results = [result for result in results if isinstance(result, dict)]
        logger.info(f"Got {len(valid_results)} valid results from shazam search collector out of {n_queries}")

        return merge_dicts(*results)

    async def _search_single_track(self, query: str) -> Dict[str, Optional[str]]:
        response = await self._shazam.search_track(query=query, limit=1)

        if isinstance(response, dict):
            hits = safe_nested_get(response, [TRACKS, HITS])
            track_id = hits[0].get(KEY) if hits else None
        else:
            logger.info(f"Did not find any search result for query `{query}`")
            track_id = None

        return {query: track_id}
