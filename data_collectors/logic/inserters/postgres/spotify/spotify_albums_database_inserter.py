from typing import List, Optional, Type

from genie_common.utils import to_datetime
from genie_datastores.postgres.inner_utils.spotify_utils import extract_artist_id
from genie_datastores.postgres.models import SpotifyAlbum, BaseORMModel, SpotifyAlbumType
from genie_datastores.postgres.models.orm.spotify.base_spotify_orm_model import (
    BaseSpotifyORMModel,
)

from data_collectors.consts.spotify_consts import (
    TRACK,
    ALBUM,
    ARTISTS,
    ID,
    ALBUMS,
    RELEASE_DATE,
    ALBUM_TYPE,
    TOTAL_TRACKS,
    NAME,
    SPOTIFY_RELEASE_DATE_ORDERED_FORMATS,
)
from data_collectors.logic.inserters.postgres import BaseIDsDatabaseInserter


class SpotifyAlbumsDatabaseInserter(BaseIDsDatabaseInserter):
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

    def _to_records(self, raw_records: List[dict]) -> List[BaseORMModel]:
        return [self._to_record(response) for response in raw_records]

    def _to_record(self, response: dict) -> SpotifyAlbum:
        album_type = self._extract_album_type(response)
        return SpotifyAlbum(
            id=response[ID],
            artist_id=extract_artist_id(response),
            group=album_type,
            name=response[NAME],
            release_date=to_datetime(response[RELEASE_DATE], SPOTIFY_RELEASE_DATE_ORDERED_FORMATS),
            total_tracks=response[TOTAL_TRACKS],
            type=album_type,
        )

    @staticmethod
    def _extract_album_type(response: dict) -> Optional[SpotifyAlbumType]:
        album_type = response.get(ALBUM_TYPE)
        return None if album_type is None else SpotifyAlbumType(album_type)

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return SpotifyAlbum

    @property
    def name(self) -> str:
        return ALBUMS
