from typing import Any, List, Optional, Tuple

from shazamio.schemas.artists import ArtistQuery
from shazamio.schemas.enums import ArtistView, ArtistExtend

from data_collectors.logic.collectors.shazam.base_shazam_collector import BaseShazamCollector
from data_collectors.logic.utils import get_all_enum_values
from data_collectors.logs import logger


class ShazamArtistsCollector(BaseShazamCollector):
    async def collect(self, ids: List[str]) -> Any:
        logger.info(f"Starting to collect {len(ids)} Shazam artists")
        return await self._pool_executor.run(
            iterable=ids,
            func=self._extract_single_track_artist,
            expected_type=dict
        )

    async def _extract_single_track_artist(self, artist_id: Tuple[dict, str]) -> Optional[dict]:
        if artist_id is None:
            logger.warning("Was not able to extract artist id from Shazam track. Ignoring")
            return

        return await self._shazam.artist_about(
            artist_id=int(artist_id),
            query=ArtistQuery(
                views=get_all_enum_values(ArtistView),
                extend=get_all_enum_values(ArtistExtend)
            )
        )
