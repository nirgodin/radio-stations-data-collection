from http import HTTPStatus
from random import randint, random
from typing import List, Dict
from unittest.mock import AsyncMock, patch

from _pytest.fixtures import fixture
from genie_common.utils import random_alphanumeric_string, random_datetime, random_enum_value
from genie_datastores.models import EntityType, DataSource
from genie_datastores.mongo.models import AboutDocument
from genie_datastores.postgres.models import Gender
from genie_datastores.postgres.operations import insert_records
from genie_datastores.testing.postgres import PostgresMockFactory
from google.generativeai import GenerativeModel
from spotipyio.testing import SpotifyMockFactory
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ArtistExtractedDetails, ArtistDetailsExtractionResponse, ArtistExistingDetails
from data_collectors.logic.models.artist_about.artist_extracted_details import (
    StringDecision,
    GenderDecision,
    DateDecision,
)
from tests.helpers.mock_generate_content_response import MockGenerateContentResponse


class TestArtistsInsightsManager:
    async def test_trigger(
        self,
        db_engine: AsyncEngine,
        artists_extraction_responses: List[ArtistDetailsExtractionResponse],
        mock_gemini_model: AsyncMock,
        test_client: TestClient,
    ):
        await self._given_artists_entries(artists_extraction_responses, db_engine)
        self._given_valid_gemini_responses(
            mock_gemini_model=mock_gemini_model,
            artists_extraction_responses=artists_extraction_responses,
        )

        with test_client as client:
            # Must be inside the context manager so Beanie is initialized by the lifespan startup
            await self._given_artists_about_documents(artists_extraction_responses)
            actual = client.post(f"/jobs/trigger/{JobId.ARTISTS_INSIGHTS.value}")

        assert actual.status_code == HTTPStatus.OK.value

    @staticmethod
    async def _given_artists_entries(
        artists_extraction_responses: List[ArtistDetailsExtractionResponse], db_engine: AsyncEngine
    ) -> None:
        artists_ids = [res.existing_details.id for res in artists_extraction_responses]
        spotify_artists = [PostgresMockFactory.spotify_artist(id=id_) for id_ in artists_ids]
        artists = [PostgresMockFactory.artist(id=id_, shazam_id=None, gender=None) for id_ in artists_ids]

        await insert_records(db_engine, spotify_artists)
        await insert_records(db_engine, artists)

    def _given_valid_gemini_responses(
        self,
        mock_gemini_model: AsyncMock,
        artists_extraction_responses: List[ArtistDetailsExtractionResponse],
    ) -> None:
        def _mock_generate_content(contents: str, generation_config: Dict[str, str]) -> MockGenerateContentResponse:
            for response in artists_extraction_responses:
                if contents.endswith(response.existing_details.about):
                    return self._generate_content_response_with(response.extracted_details.gender)

        mock_gemini_model.side_effect = _mock_generate_content

    @staticmethod
    def _generate_content_response_with(gender_decision: GenderDecision) -> MockGenerateContentResponse:
        serialize_response = ArtistExtractedDetails(
            birth_date=None,
            death_date=None,
            origin=None,
            gender=gender_decision,
        )
        response_json = serialize_response.json()

        return MockGenerateContentResponse(
            parts=[response_json],
            text=response_json,
        )

    async def _given_artists_about_documents(
        self, artists_extraction_responses: List[ArtistDetailsExtractionResponse]
    ) -> None:
        about_documents = [self._random_about_document_with(res) for res in artists_extraction_responses]
        await AboutDocument.insert_many(about_documents)

    @staticmethod
    def _random_about_document_with(extraction_response: ArtistDetailsExtractionResponse) -> AboutDocument:
        return AboutDocument(
            about=extraction_response.existing_details.about,
            entity_type=EntityType.ARTIST,
            entity_id=extraction_response.existing_details.id,
            name=random_alphanumeric_string(),
            source=DataSource.WIKIPEDIA,
        )

    def _random_artist_details_extraction_response(self) -> ArtistDetailsExtractionResponse:
        return ArtistDetailsExtractionResponse(
            existing_details=self._random_artist_existing_details(),
            extracted_details=self._random_artist_extracted_details(),
            data_source=DataSource.WIKIPEDIA,
        )

    @staticmethod
    def _random_artist_existing_details() -> ArtistExistingDetails:
        return ArtistExistingDetails(
            id=SpotifyMockFactory.spotify_id(),
            about=random_alphanumeric_string(20, 50),
            origin=random_alphanumeric_string(),
            birth_date=random_datetime(),
            death_date=random_datetime(),
            gender=None,
        )

    @staticmethod
    def _random_artist_extracted_details() -> ArtistExtractedDetails:
        return ArtistExtractedDetails(
            birth_date=DateDecision(
                value=random_datetime(), evidence=random_alphanumeric_string(), confidence=random()
            ),
            death_date=DateDecision(
                value=random_datetime(), evidence=random_alphanumeric_string(), confidence=random()
            ),
            origin=StringDecision(
                value=random_alphanumeric_string(), evidence=random_alphanumeric_string(), confidence=random()
            ),
            gender=GenderDecision(
                value=random_enum_value(Gender), evidence=random_alphanumeric_string(), confidence=random()
            ),
        )

    @fixture
    def artists_extraction_responses(self) -> List[ArtistDetailsExtractionResponse]:
        return [self._random_artist_details_extraction_response() for _ in range(randint(1, 10))]

    @fixture
    def mock_gemini_model(self) -> AsyncMock:
        with patch.object(GenerativeModel, "generate_content_async") as mock_model:
            yield mock_model
