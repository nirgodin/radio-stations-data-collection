from typing import Any, List, Dict, Optional

from shazamio.schemas.artists import ArtistQuery
from shazamio.schemas.enums import ArtistView, ArtistExtend

from data_collectors.consts.shazam_consts import ADAM_ID, KEY
from data_collectors.consts.spotify_consts import ARTISTS
from data_collectors.logic.collectors.shazam.base_shazam_collector import BaseShazamCollector
from data_collectors.logic.utils import get_all_enum_values
from data_collectors.logs import logger


class ShazamArtistsCollector(BaseShazamCollector):
    async def collect(self, tracks: List[dict]) -> Any:
        logger.info(f"Starting to collect Shazam artists for {len(tracks)} tracks")
        results = await self._pool_executor.run(iterable=tracks, func=self._extract_single_track_artist)
        valid_results = [result for result in results if isinstance(result, dict)]
        logger.info(f"Successfully retrieved {len(valid_results)} Shazam artists from {len(tracks)} tracks")

        return valid_results

    async def _extract_single_track_artist(self, track: dict) -> Optional[Dict[str, dict]]:
        artist_id = self._extract_artist_id(track)
        if artist_id is None:
            logger.warning("Was not able to extract artist id from Shazam track. Ignoring")
            return

        response = await self._shazam.artist_about(
            artist_id=int(artist_id),
            query=ArtistQuery(
                views=get_all_enum_values(ArtistView),
                extend=get_all_enum_values(ArtistExtend)
            )
        )
        track_id = track[KEY]

        return {track_id: response}

    @staticmethod
    def _extract_artist_id(track: dict) -> Optional[str]:
        artists = track.get(ARTISTS, [])

        if artists:
            first_artist = artists[0]
            return first_artist.get(ADAM_ID)
