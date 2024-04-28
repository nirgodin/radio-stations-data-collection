from typing import List, Dict, Optional

from aiohttp import ClientSession
from genie_common.tools import AioPoolExecutor, logger
from genie_common.utils import merge_dicts, decode_image
from genie_common.clients.utils import fetch_image
from numpy import ndarray
from spotipyio import SpotifyClient

from data_collectors.consts.spotify_consts import IMAGES, URL, ID
from data_collectors.contract import ICollector


class SpotifyArtistsImagesCollector(ICollector):
    def __init__(self, client_session: ClientSession, spotify_client: SpotifyClient, pool_executor: AioPoolExecutor):
        self._client_session = client_session
        self._spotify_client = spotify_client
        self._pool_executor = pool_executor

    async def collect(self, ids: List[str]) -> Dict[str, ndarray]:
        unique_ids = list(set(ids))
        n_artists = len(unique_ids)
        logger.info(f"Starting to collect images of {n_artists} artists")
        artists = await self._spotify_client.artists.info.run(unique_ids)
        ids_to_images = await self._pool_executor.run(
            iterable=artists,
            func=self._collect_single_artist_image,
            expected_type=dict
        )
        logger.info(f"Successfully collected {len(ids_to_images)} images out of {n_artists} requests")

        return merge_dicts(*ids_to_images)

    async def _collect_single_artist_image(self, artist: dict) -> Optional[Dict[str, ndarray]]:
        images = artist.get(IMAGES)
        if not images:
            return

        first_image_url = images[0][URL]
        image_bytes = await fetch_image(session=self._client_session, url=first_image_url, wrap_exceptions=False)
        image = decode_image(image_bytes)

        return {artist[ID]: image}
