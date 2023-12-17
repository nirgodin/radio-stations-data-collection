from typing import Dict, Optional

from shazamio import Shazam

from data_collectors.consts.shazam_consts import HITS, KEY
from data_collectors.consts.spotify_consts import TRACKS
from data_collectors.contract import BaseSearchCollector
from data_collectors.logic.collectors.shazam.base_shazam_collector import BaseShazamCollector
from data_collectors.logic.models import MissingTrack
from genie_common.utils import safe_nested_get
from genie_common.tools import logger
from genie_common.tools import AioPoolExecutor


class ShazamSearchCollector(BaseSearchCollector, BaseShazamCollector):
    def __init__(self, shazam: Shazam, pool_executor: AioPoolExecutor):
        super(BaseShazamCollector, self).__init__()
        super(BaseSearchCollector, self).__init__(shazam, pool_executor)

    def _to_query(self, missing_track: MissingTrack) -> str:
        return f"{missing_track.artist_name} - {missing_track.track_name}"

    async def _search_single_track(self, missing_track: MissingTrack) -> Dict[MissingTrack, Optional[str]]:
        query = self._to_query(missing_track)
        response = await self._shazam.search_track(query=query, limit=1)

        if isinstance(response, dict):
            hits = safe_nested_get(response, [TRACKS, HITS])
            track_id = hits[0].get(KEY) if hits else None
        else:
            logger.info(f"Did not find any search result for query `{query}`")
            track_id = None

        return {missing_track: track_id}
