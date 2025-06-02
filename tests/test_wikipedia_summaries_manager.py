from functools import partial
from http import HTTPStatus
from random import randint
from typing import List
from urllib.parse import urlencode

from _pytest.fixtures import fixture
from genie_common.utils import random_alphanumeric_string
from genie_datastores.models import DataSource
from genie_datastores.mongo.models import AboutDocument
from genie_datastores.postgres.models import SpotifyArtist
from genie_datastores.postgres.operations import insert_records
from genie_datastores.testing.postgres import PostgresMockFactory
from responses import RequestsMock
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import WikipediaArtistAbout
from tests.testing_utils import until


class TestWikipediaSummariesManager:
    async def test_trigger(
        self,
        spotify_artists: List[SpotifyArtist],
        db_engine: AsyncEngine,
        wikipedia_artists_abouts: List[WikipediaArtistAbout],
        test_client: TestClient,
        mock_responses: RequestsMock,
    ):
        await insert_records(db_engine, spotify_artists)
        await self._given_valid_wikipedia_responses(mock_responses, wikipedia_artists_abouts)

        with test_client as client:
            actual = client.post(f"/jobs/trigger/{JobId.WIKIPEDIA_SUMMARIES.value}")

        assert actual.status_code == HTTPStatus.OK
        await until(partial(self._are_all_documents_were_inserted, wikipedia_artists_abouts))

    @staticmethod
    async def _given_valid_wikipedia_responses(
        mock_responses: RequestsMock, artists_abouts: List[WikipediaArtistAbout]
    ) -> None:
        for about in artists_abouts:
            params = {
                "action": "query",
                "prop": "extracts",
                "titles": about.wikipedia_name,
                "explaintext": 1,
                "exsectionformat": "wiki",
                "format": "json",
                "redirects": 1,
            }
            querystring = urlencode(params)
            mock_responses.get(
                url=f"https://{about.wikipedia_language.lower()}.wikipedia.org/w/api.php?{querystring}",
                json={"query": {"pages": {str(randint(1, 10000)): {"extract": about.about}}}},
            )

    @fixture
    def wikipedia_artists_abouts(self, spotify_artists: List[SpotifyArtist]) -> List[WikipediaArtistAbout]:
        abouts = []

        for artist in spotify_artists:
            artist_about = WikipediaArtistAbout.from_row(artist)
            artist_about.about = random_alphanumeric_string()
            abouts.append(artist_about)

        return abouts

    @fixture
    def spotify_artists(self) -> List[SpotifyArtist]:
        return [PostgresMockFactory.spotify_artist() for _ in range(randint(1, 10))]

    async def _are_all_documents_were_inserted(self, wikipedia_artists_abouts: List[WikipediaArtistAbout]) -> bool:
        for artist_about in wikipedia_artists_abouts:
            if not await self._is_existing_document(artist_about):
                return False

        return True

    @staticmethod
    async def _is_existing_document(artist_about: WikipediaArtistAbout) -> bool:
        expected = artist_about.to_about_document()
        expected.creation_date = None
        expected.update_date = None
        actual = await AboutDocument.find_one(
            AboutDocument.entity_id == artist_about.id,
            AboutDocument.source == DataSource.WIKIPEDIA,
        )
        actual.id = None
        actual.creation_date = None
        actual.update_date = None

        return actual == expected
