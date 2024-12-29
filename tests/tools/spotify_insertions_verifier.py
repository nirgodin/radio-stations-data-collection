from asyncio import gather
from typing import List

from genie_datastores.postgres.models import (
    SpotifyArtist,
    SpotifyTrack,
    AudioFeatures,
    Track,
    TrackIDMapping,
    SpotifyAlbum,
)
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine


class SpotifyInsertionsVerifier:
    def __init__(self, db_engine: AsyncEngine):
        self._db_engine = db_engine

    async def verify(
        self, artists: List[str], tracks: List[str], albums: List[str]
    ) -> bool:
        verification_coroutines = [
            self._inserted_expected_spotify_artists_records(artists),
            self._inserted_expected_artists_records(artists),
            self._inserted_expected_spotify_tracks_records(tracks),
            self._inserted_expected_tracks_records(tracks),
            self._inserted_expected_audio_features_records(tracks),
            self._inserted_expected_albums_records(tracks),
            self._inserted_expected_albums_records(albums),
        ]
        actual = await gather(*verification_coroutines)

        return all(actual)

    async def _inserted_expected_spotify_artists_records(
        self, expected: List[str]
    ) -> bool:
        query_result = await execute_query(
            engine=self._db_engine, query=select(SpotifyArtist.id)
        )
        actual = query_result.scalars().all()

        return sorted(expected) == sorted(actual)

    async def _inserted_expected_artists_records(self, expected: List[str]) -> bool:
        query_result = await execute_query(
            engine=self._db_engine, query=select(SpotifyArtist.id)
        )
        actual = query_result.scalars().all()

        return sorted(expected) == sorted(actual)

    async def _inserted_expected_spotify_tracks_records(
        self, expected: List[str]
    ) -> bool:
        query_result = await execute_query(
            engine=self._db_engine, query=select(SpotifyTrack.id)
        )
        actual = query_result.scalars().all()

        return sorted(expected) == sorted(actual)

    async def _inserted_expected_audio_features_records(
        self, expected: List[str]
    ) -> bool:
        query_result = await execute_query(
            engine=self._db_engine, query=select(AudioFeatures.id)
        )
        actual = query_result.scalars().all()

        return sorted(expected) == sorted(actual)

    async def _inserted_expected_tracks_records(self, expected: List[str]) -> bool:
        query_result = await execute_query(
            engine=self._db_engine, query=select(Track.id)
        )
        actual = query_result.scalars().all()

        return sorted(expected) == sorted(actual)

    async def _inserted_expected_track_id_mapping_records(
        self, expected: List[List[str]]
    ) -> bool:
        query_result = await execute_query(
            engine=self._db_engine, query=select(TrackIDMapping.id)
        )
        actual = query_result.scalars().all()

        return sorted(expected) == sorted(actual)

    async def _inserted_expected_albums_records(self, expected: List[str]) -> bool:
        query_result = await execute_query(
            engine=self._db_engine, query=select(SpotifyAlbum.id)
        )
        actual = query_result.scalars().all()

        return sorted(expected) == sorted(actual)