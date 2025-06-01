from http import HTTPStatus
from random import randint
from typing import List

import responses
from _pytest.fixtures import fixture
from aioresponses import aioresponses
from genie_common.utils import random_alphanumeric_string
from genie_datastores.postgres.models import SpotifyArtist
from genie_datastores.postgres.operations import insert_records
from genie_datastores.testing.postgres import PostgresMockFactory
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import WikipediaArtistAbout


class TestWikipediaSummariesManager:
    @responses.activate
    async def test_trigger(
        self,
        spotify_artists: List[SpotifyArtist],
        db_engine: AsyncEngine,
        wikipedia_artists_abouts: List[WikipediaArtistAbout],
        test_client: TestClient,
        # mock_responses: aioresponses,
    ):
        await insert_records(db_engine, spotify_artists)
        await self._given_valid_wikipedia_responses(wikipedia_artists_abouts)

        with test_client as client:
            actual = client.post(f"/jobs/trigger/{JobId.WIKIPEDIA_SUMMARIES.value}")

        assert actual.status_code == HTTPStatus.OK

    async def test_scheduled(self):
        pass

    @staticmethod
    async def _given_valid_wikipedia_responses(artists_abouts: List[WikipediaArtistAbout]) -> None:
        urls = []

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
            normalized_params = "&".join([f"{k}={str(v)}" for k, v in params.items()])
            url = f"https://{about.wikipedia_language.lower()}.wikipedia.org/w/api.php?{normalized_params}"
            urls.append(url)

            responses.get(
                url=url,
                # params={
                #     'action': 'query',
                #     'explaintext': 1,
                #     'exsectionformat': 'wiki',
                #     'format': 'json',
                #     'prop': 'extracts',
                #     'redirects': 1,
                #     'titles': about.wikipedia_name,
                # },
                json={"query": {"pages": {"extract": random_alphanumeric_string()}}},
            )

        print(urls)

    @fixture
    def wikipedia_artists_abouts(self, spotify_artists: List[SpotifyArtist]) -> List[WikipediaArtistAbout]:
        return [WikipediaArtistAbout.from_row(row) for row in spotify_artists]

    @fixture
    def spotify_artists(self) -> List[SpotifyArtist]:
        return [PostgresMockFactory.spotify_artist() for _ in range(randint(1, 10))]
