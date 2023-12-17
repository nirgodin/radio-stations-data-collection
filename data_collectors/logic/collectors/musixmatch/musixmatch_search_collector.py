from typing import Optional, Dict

from aiohttp import ClientSession

from data_collectors.contract import BaseSearchCollector
from data_collectors.logic.collectors.musixmatch.base_musixmatch_collector import BaseMusixmatchCollector
from data_collectors.logic.collectors.musixmatch.musixmatch_search_results_extractor import \
    MusixmatchSearchResultsExtractor
from data_collectors.logic.models import MissingTrack
from genie_common.tools import AioPoolExecutor


class MusixmatchSearchCollector(BaseSearchCollector, BaseMusixmatchCollector):
    def __init__(self, session: ClientSession, pool_executor: AioPoolExecutor, api_key: str):
        super(BaseSearchCollector, self).__init__(session, pool_executor, api_key)
        super(BaseMusixmatchCollector, self).__init__()

    async def _search_single_track(self, missing_track: MissingTrack) -> Dict[MissingTrack, Optional[str]]:
        response = await self._get(params=self._to_query(missing_track))
        track_id = MusixmatchSearchResultsExtractor.extract(response, missing_track)

        return {missing_track: track_id}

    def _to_query(self, missing_track: MissingTrack) -> dict:
        return {
            "q_artist": missing_track.artist_name,
            "q_track": missing_track.track_name
        }

    @property
    def _route(self) -> str:
        return "track.search"
