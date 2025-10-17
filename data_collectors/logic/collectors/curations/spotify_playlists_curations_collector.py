from typing import List, Dict, Any, Generator

from genie_datastores.models import DataSource
from spotipyio import SpotifyClient

from data_collectors.contract import ICollector
from data_collectors.logic.models import Curation


class SpotifyPlaylistsCurationsCollector(ICollector):
    def __init__(self, spotify_client: SpotifyClient):
        self._spotify_client = spotify_client

    async def collect(self, playlists_ids: List[str]) -> List[Curation]:
        playlists = await self._spotify_client.playlists.info.run(
            ids=playlists_ids,
            max_pages=100,
        )
        curations = []

        for playlist in playlists:
            tracks_ids = self._extract_playlist_tracks(playlist)
            playlist_curations = list(self._to_curations(playlist, tracks_ids))
            curations.extend(playlist_curations)

        return curations

    def _extract_playlist_tracks(self, playlist: Dict[str, Any]) -> List[str]:
        print("b")

    def _to_curations(self, playlist: Dict[str, Any], tracks_ids: List[str]) -> Generator[Curation, None, None]:
        curator_id = ""
        collection_id = ""
        curator_name = ""
        collection_date = ""
        collection_name = ""
        collection_description = ""

        for track_id in tracks_ids:
            yield Curation(
                collection_id=collection_id,
                curator_id=curator_id,
                curator_name=curator_name,
                date=collection_date,
                description=collection_description,
                source=DataSource.JOSIE,
                title=collection_name,
                track_id=track_id,
                collection_comment="",
            )
