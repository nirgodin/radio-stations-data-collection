from typing import Any, List, Optional, Tuple

from shazamio.schemas.artists import ArtistQuery
from shazamio.schemas.enums import ArtistView, ArtistExtend

from data_collectors.logic.collectors.shazam.base_shazam_collector import BaseShazamCollector
from data_collectors.logic.utils import get_all_enum_values
from data_collectors.logs import logger


class ShazamArtistsCollector(BaseShazamCollector):
    async def collect(self, ids: List[str]) -> Any:
        logger.info(f"Starting to collect {len(ids)} Shazam artists")
        results = await self._pool_executor.run(iterable=ids, func=self._extract_single_track_artist)
        valid_results = [result for result in results if isinstance(result, dict)]
        logger.info(f"Successfully retrieved {len(valid_results)} Shazam artists from {len(ids)} tracks")

        return valid_results

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
