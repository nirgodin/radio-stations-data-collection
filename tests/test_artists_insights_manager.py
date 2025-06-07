from http import HTTPStatus
from random import randint
from typing import List
from unittest.mock import AsyncMock, patch

from _pytest.fixtures import fixture
from genie_common.utils import random_alphanumeric_string
from genie_datastores.models import EntityType, DataSource
from genie_datastores.mongo.models import AboutDocument
from genie_datastores.postgres.operations import insert_records
from genie_datastores.testing.postgres import PostgresMockFactory
from google.generativeai import GenerativeModel
from spotipyio.testing import SpotifyMockFactory
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.jobs.job_id import JobId


class TestArtistsInsightsManager:
    async def test_trigger(
        self,
        db_engine: AsyncEngine,
        artists_ids: List[str],
        mock_gemini_model: AsyncMock,
        test_client: TestClient,
    ):
        await self._given_artists_entries(artists_ids, db_engine)
        mock_gemini_model.return_value = "bla bla"
        with test_client as client:
            await self._given_artists_about_documents(artists_ids)

            actual = client.post(f"/jobs/trigger/{JobId.ARTISTS_INSIGHTS.value}")

        assert actual.status_code == HTTPStatus.OK.value

    @staticmethod
    async def _given_artists_entries(artists_ids: List[str], db_engine: AsyncEngine) -> None:
        spotify_artists = [PostgresMockFactory.spotify_artist(id=id_) for id_ in artists_ids]
        artists = [PostgresMockFactory.artist(id=id_, shazam_id=None, gender=None) for id_ in artists_ids]

        await insert_records(db_engine, spotify_artists)
        await insert_records(db_engine, artists)

    async def _given_artists_about_documents(self, artists_ids: List[str]) -> None:
        about_documents = [self._random_about_document_with(id_) for id_ in artists_ids]
        await AboutDocument.insert_many(about_documents)

    @staticmethod
    def _random_about_document_with(artist_id: str) -> AboutDocument:
        return AboutDocument(
            about=random_alphanumeric_string(),
            entity_type=EntityType.ARTIST,
            entity_id=artist_id,
            name=random_alphanumeric_string(),
            source=DataSource.WIKIPEDIA,
        )

    @fixture
    def artists_ids(self) -> List[str]:
        return [SpotifyMockFactory.spotify_id() for _ in range(randint(1, 10))]

    @fixture
    def mock_gemini_model(self) -> AsyncMock:
        with patch.object(GenerativeModel, "generate_content_async") as mock_model:
            yield mock_model
