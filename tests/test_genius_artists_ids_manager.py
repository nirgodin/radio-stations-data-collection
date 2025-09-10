from http import HTTPStatus
from random import randint
from typing import List

from _pytest.fixtures import fixture
from genie_datastores.postgres.models import Artist
from genie_datastores.postgres.operations import insert_records, execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.jobs.job_id import JobId
from tests.helpers.genius_artist_resources import GeniusArtistResources
from tests.tools.test_clients.genius_test_client import GeniusTestClient


class TestGeniusArtistsIDsManager:
    async def test_trigger(
        self,
        test_client: TestClient,
        db_engine: AsyncEngine,
        genius_artists_resources: List[GeniusArtistResources],
        genius_test_client: GeniusTestClient,
    ):
        await self._given_artists_records_without_genius_id(genius_artists_resources, db_engine)
        self._given_expected_genius_search_responses(genius_test_client, genius_artists_resources)

        with test_client as client:
            actual = client.post(f"/jobs/trigger/{JobId.GENIUS_ARTISTS_IDS_IMPUTER.value}")

        assert actual.status_code == HTTPStatus.OK.value
        await self._assert_expected_genius_ids_stored(db_engine, genius_artists_resources)

    @staticmethod
    async def _given_artists_records_without_genius_id(
        genius_artists_resources: List[GeniusArtistResources], db_engine: AsyncEngine
    ) -> None:
        spotify_artist_records = [resource.to_spotify_artist() for resource in genius_artists_resources]
        await insert_records(engine=db_engine, records=spotify_artist_records)
        artist_records = [resource.to_artist() for resource in genius_artists_resources]
        await insert_records(engine=db_engine, records=artist_records)

    @fixture
    def genius_artists_resources(self) -> List[GeniusArtistResources]:
        return [GeniusArtistResources.random() for _ in range(randint(1, 10))]

    @staticmethod
    def _given_expected_genius_search_responses(
        genius_test_client: GeniusTestClient, genius_artists_resources: List[GeniusArtistResources]
    ) -> None:
        for resource in genius_artists_resources:
            genius_test_client.expect_search_artist_request(name=resource.name, response=resource.to_search_response())

    @staticmethod
    async def _assert_expected_genius_ids_stored(
        db_engine: AsyncEngine, genius_artists_resources: List[GeniusArtistResources]
    ) -> None:
        for resource in genius_artists_resources:
            query = select(Artist.genius_id).where(Artist.id == resource.spotify_id)
            cursor = await execute_query(engine=db_engine, query=query)

            actual = cursor.scalars().first()

            assert actual == resource.genius_id
