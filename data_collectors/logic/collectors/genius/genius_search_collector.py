import asyncio
from typing import Dict, Optional

from aiohttp import ClientSession

from data_collectors.components.sessions_component_factory import SessionsComponentFactory
from data_collectors.consts.genius_consts import RESPONSE, RESULT
from data_collectors.consts.shazam_consts import HITS
from data_collectors.consts.spotify_consts import ID
from data_collectors.contract import BaseSearchCollector
from data_collectors.logic.collectors.genius.base_genius_collector import BaseGeniusCollector
from data_collectors.logic.models import MissingTrack
from data_collectors.logic.utils import safe_nested_get
from data_collectors.tools import AioPoolExecutor


class GeniusSearchCollector(BaseSearchCollector, BaseGeniusCollector):
    def __init__(self, session: ClientSession, pool_executor: AioPoolExecutor):
        super(BaseSearchCollector, self).__init__(session)
        super(BaseGeniusCollector, self).__init__()
        self._pool_executor = pool_executor

    async def _search_single_track(self, missing_track: MissingTrack) -> Dict[MissingTrack, Optional[str]]:
        params = self._to_query(missing_track)
        response = await self._get(params)

        if self._is_valid_response(response):
            track_id = self._serialize_response(response)
        else:
            track_id = None

        return {missing_track: track_id}

    @staticmethod
    def _serialize_response(response: dict) -> Optional[str]:
        hits = safe_nested_get(response, [RESPONSE, HITS])

        if hits:
            first_hit = hits[0]
            track_id = safe_nested_get(first_hit, [RESULT, ID])

            return None if track_id is None else str(track_id)

    def _to_query(self, missing_track: MissingTrack) -> Dict[str, str]:
        query = f"{missing_track.artist_name} - {missing_track.track_name}"
        return {"q": query}

    @property
    def _route(self) -> str:
        return "search"


async def main():
    client_session = SessionsComponentFactory().get_genius_session()
    async with client_session as session:
        collector = GeniusSearchCollector(session, AioPoolExecutor())
        await collector.collect([MissingTrack(spotify_id="", artist_name="Queen", track_name="Radio Ga Ga")])

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())