from functools import partial
from http import HTTPStatus
from random import randint, random
from typing import List, Dict, Tuple
from unittest.mock import AsyncMock

from _pytest.fixtures import fixture
from genie_common.utils import random_alphanumeric_string, random_datetime, random_enum_value
from genie_datastores.models import EntityType, DataSource
from genie_datastores.mongo.models import AboutDocument
from genie_datastores.postgres.models import Gender, Artist, Decision, Table, BaseORMModel
from genie_datastores.postgres.operations import insert_records, execute_query
from genie_datastores.testing.postgres import PostgresMockFactory
from spotipyio.testing import SpotifyMockFactory
from sqlalchemy import select, inspect
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.components import ComponentFactory
from data_collectors.jobs.job_builders.artists_insights_job_builder import ArtistsInsightsJobBuilder
from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ArtistExtractedDetails, ArtistDetailsExtractionResponse, ArtistExistingDetails
from data_collectors.logic.models.artist_about.artist_extracted_details import (
    StringDecision,
    GenderDecision,
    DateDecision,
)
from tests.helpers.mock_generate_content_response import MockGenerateContentResponse
from tests.testing_utils import build_scheduled_test_client, until


class TestArtistsInsightsManager:
    async def test_trigger(
        self,
        db_engine: AsyncEngine,
        artists_extraction_responses: List[ArtistDetailsExtractionResponse],
        mock_gemini_model: AsyncMock,
        test_client: TestClient,
    ):
        with test_client as client:
            # Must be inside the context manager so Beanie is initialized by the lifespan startup
            await self._given_expected_db_entries_and_gemini_responses(
                artists_extraction_responses=artists_extraction_responses,
                db_engine=db_engine,
                mock_gemini_model=mock_gemini_model,
            )

            actual = client.post(f"/jobs/trigger/{JobId.ARTISTS_INSIGHTS.value}")

        assert actual.status_code == HTTPStatus.OK.value
        assert await self._inserted_expected_db_records(
            db_engine=db_engine, artists_extraction_responses=artists_extraction_responses
        )

    async def test_scheduled(
        self,
        db_engine: AsyncEngine,
        artists_extraction_responses: List[ArtistDetailsExtractionResponse],
        mock_gemini_model: AsyncMock,
        scheduled_test_client: TestClient,
    ):
        condition = partial(
            self._inserted_expected_db_records,
            db_engine=db_engine,
            artists_extraction_responses=artists_extraction_responses,
        )
        with scheduled_test_client:
            # Must be inside the context manager so Beanie is initialized by the lifespan startup
            await self._given_expected_db_entries_and_gemini_responses(
                artists_extraction_responses=artists_extraction_responses,
                db_engine=db_engine,
                mock_gemini_model=mock_gemini_model,
            )

            await until(condition)

    async def _given_expected_db_entries_and_gemini_responses(
        self,
        artists_extraction_responses: List[ArtistDetailsExtractionResponse],
        db_engine: AsyncEngine,
        mock_gemini_model: AsyncMock,
    ) -> None:
        await self._given_artists_entries(artists_extraction_responses, db_engine)
        self._given_valid_gemini_responses(
            mock_gemini_model=mock_gemini_model,
            artists_extraction_responses=artists_extraction_responses,
        )
        await self._given_artists_about_documents(artists_extraction_responses)

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

    async def _inserted_expected_db_records(
        self, db_engine: AsyncEngine, artists_extraction_responses: List[ArtistDetailsExtractionResponse]
    ) -> bool:
        for artist in artists_extraction_responses:
            updated_expected_artist_record = await self._updated_expected_artist_records(db_engine, artist)
            inserted_expected_decision_record = await self._inserted_expected_decision_record(db_engine, artist)

            if not (updated_expected_artist_record and inserted_expected_decision_record):
                return False

        return True

    @staticmethod
    async def _updated_expected_artist_records(db_engine: AsyncEngine, artist: ArtistDetailsExtractionResponse) -> bool:
        query = select(Artist.gender).where(Artist.id == artist.existing_details.id)
        query_result = await execute_query(db_engine, query)
        actual = query_result.scalar()

        return actual == artist.extracted_details.gender.value

    async def _inserted_expected_decision_record(
        self, db_engine: AsyncEngine, artist: ArtistDetailsExtractionResponse
    ) -> bool:
        expected = Decision(
            column="gender",
            source=DataSource.WIKIPEDIA,
            table=Table.ARTISTS,
            table_id=artist.existing_details.id,
            confidence=artist.extracted_details.gender.confidence,
            evidence=artist.extracted_details.gender.evidence,
        )
        query = select(Decision).where(Decision.table_id == artist.existing_details.id)
        query_result = await execute_query(db_engine, query)
        actual = query_result.scalars().all()

        if len(actual) != 1:
            return False

        return self._are_identical_records(actual[0], expected, ignore_columns=("id", "creation_date", "update_date"))

    @staticmethod
    def _are_identical_records(
        actual: BaseORMModel, expected: BaseORMModel, ignore_columns: Tuple[str, ...] = ()
    ) -> bool:
        if type(actual) is not type(expected):
            raise ValueError("Cannot compare objects of different types.")

        for column in inspect(actual.__class__).columns:
            if column.key not in ignore_columns:
                if getattr(actual, column.key) != getattr(expected, column.key):
                    return False

        return True

    @fixture
    def artists_extraction_responses(self) -> List[ArtistDetailsExtractionResponse]:
        return [self._random_artist_details_extraction_response() for _ in range(randint(1, 10))]

    @fixture
    async def scheduled_test_client(
        self,
        component_factory: ComponentFactory,
    ) -> TestClient:
        scheduled_client = await build_scheduled_test_client(component_factory, ArtistsInsightsJobBuilder)
        with scheduled_client as client:
            yield client
