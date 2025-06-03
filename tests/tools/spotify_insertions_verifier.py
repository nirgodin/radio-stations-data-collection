from asyncio import gather
from typing import List

from genie_common.utils import chain_lists
from genie_datastores.postgres.models import (
    SpotifyArtist,
    SpotifyTrack,
    AudioFeatures,
    Track,
    TrackIDMapping,
    SpotifyAlbum,
    Artist,
)
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.helpers.spotify_playlists_resources import SpotifyPlaylistsResources


class SpotifyInsertionsVerifier:
    def __init__(self, db_engine: AsyncEngine):
        self._db_engine = db_engine

    async def verify_playlist_resources(self, resources: List[SpotifyPlaylistsResources]) -> bool:
        tracks = chain_lists(
            [resource.track_ids for resource in resources],
        )
        artists = chain_lists(
            [resource.artist_ids for resource in resources],
        )
        albums = chain_lists(
            [resource.album_ids for resource in resources],
        )

        return await self.verify(
            artists=artists,
            tracks=tracks,
            albums=albums,
        )

    async def verify(self, artists: List[str], tracks: List[str], albums: List[str]) -> bool:
        verification_coroutines = [
            self._inserted_expected_spotify_artists_records(artists),
            self._inserted_expected_artists_records(artists),
            self._inserted_expected_spotify_tracks_records(tracks),
            self._inserted_expected_tracks_records(tracks),
            self._inserted_expected_audio_features_records(tracks),
            self._inserted_expected_track_id_mapping_records(tracks),
            self._inserted_expected_albums_records(albums),
        ]
        actual = await gather(*verification_coroutines)

        return all(actual)

    async def _inserted_expected_spotify_artists_records(self, expected: List[str]) -> bool:
        query_result = await execute_query(engine=self._db_engine, query=select(SpotifyArtist.id))
        actual = query_result.scalars().all()

        return sorted(expected) == sorted(actual)

    async def _inserted_expected_artists_records(self, expected: List[str]) -> bool:
        query_result = await execute_query(engine=self._db_engine, query=select(Artist.id))
        actual = query_result.scalars().all()

        return sorted(expected) == sorted(actual)

    async def _inserted_expected_spotify_tracks_records(self, expected: List[str]) -> bool:
        query_result = await execute_query(engine=self._db_engine, query=select(SpotifyTrack.id))
        actual = query_result.scalars().all()

        return sorted(expected) == sorted(actual)

    async def _inserted_expected_audio_features_records(self, expected: List[str]) -> bool:
        query_result = await execute_query(engine=self._db_engine, query=select(AudioFeatures.id))
        actual = query_result.scalars().all()

        return sorted(expected) == sorted(actual)

    async def _inserted_expected_tracks_records(self, expected: List[str]) -> bool:
        query_result = await execute_query(engine=self._db_engine, query=select(Track.id))
        actual = query_result.scalars().all()

        return sorted(expected) == sorted(actual)

    async def _inserted_expected_track_id_mapping_records(self, expected: List[str]) -> bool:
        query_result = await execute_query(engine=self._db_engine, query=select(TrackIDMapping.id))
        actual = query_result.scalars().all()

        return sorted(expected) == sorted(actual)

    async def _inserted_expected_albums_records(self, expected: List[str]) -> bool:
        query_result = await execute_query(engine=self._db_engine, query=select(SpotifyAlbum.id))
        actual = query_result.scalars().all()

        return sorted(expected) == sorted(actual)
