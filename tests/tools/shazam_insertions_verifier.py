from asyncio import gather
from typing import Set

from genie_datastores.postgres.models import (
    ShazamArtist,
    ShazamTrack,
)
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine


class ShazamInsertionsVerifier:
    def __init__(self, db_engine: AsyncEngine):
        self._db_engine = db_engine

    async def verify(self, artists: Set[str], tracks: Set[str]) -> bool:
        verification_coroutines = [
            self._inserted_expected_artists_records(artists),
            self._inserted_expected_tracks_records(tracks),
        ]
        actual = await gather(*verification_coroutines)

        return all(actual)

    async def _inserted_expected_artists_records(self, expected: Set[str]) -> bool:
        query_result = await execute_query(engine=self._db_engine, query=select(ShazamArtist.id))
        actual = query_result.scalars().all()

        return sorted(expected) == sorted(actual)

    async def _inserted_expected_tracks_records(self, expected: Set[str]) -> bool:
        query_result = await execute_query(engine=self._db_engine, query=select(ShazamTrack.id))
        actual = query_result.scalars().all()

        return sorted(expected) == sorted(actual)
