from typing import List, Optional, Type

from genie_datastores.postgres.models.orm.spotify.base_spotify_orm_model import (
    BaseSpotifyORMModel,
)
from genie_datastores.postgres.models import SpotifyAlbum
from genie_datastores.postgres.inner_utils.spotify_utils import extract_artist_id

from data_collectors.consts.spotify_consts import TRACK, ALBUM, ARTISTS, ID, ALBUMS
from data_collectors.logic.inserters.postgres.spotify.base_spotify_database_inserter import (
    BaseSpotifyDatabaseInserter,
)


class SpotifyAlbumsDatabaseInserter(BaseSpotifyDatabaseInserter):
    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        albums = []

        for track in tracks:
            album = self._extract_single_album(track)

            if album is not None:
                albums.append(album)

        return albums

    @staticmethod
    def _extract_single_album(track: dict) -> Optional[dict]:
        inner_track = track.get(TRACK, {})
        album = inner_track.get(ALBUM) if isinstance(inner_track, dict) else None
        artist_id = extract_artist_id(inner_track)

        if isinstance(album, dict):
            artists = album.get(ARTISTS, [])

            if artists and artist_id is not None:
                album[ARTISTS][0][ID] = artist_id
                return album

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return SpotifyAlbum

    @property
    def name(self) -> str:
        return ALBUMS
