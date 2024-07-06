from functools import partial
from textwrap import dedent
from typing import List

from genie_common.tools import AioPoolExecutor, logger
from genie_datastores.models import DataSource
from google.generativeai import GenerativeModel
from pydantic import ValidationError

from data_collectors.contract import ICollector
from data_collectors.logic.models import ArtistExtractedDetails, ArtistExistingDetails, ArtistDetailsExtractionResponse


class GeminiArtistsAboutParsingCollector(ICollector):
    def __init__(self, pool_executor: AioPoolExecutor, model: GenerativeModel):
        self._pool_executor = pool_executor
        self._model = model

    async def collect(self,
                      existing_details: List[ArtistExistingDetails],
                      data_source: DataSource) -> List[ArtistDetailsExtractionResponse]:
        logger.info(f"Sending {len(existing_details)} extraction requests to Gemini")
        return await self._pool_executor.run(
            iterable=existing_details,
            func=partial(self._extract_artist_details, data_source),
            expected_type=ArtistDetailsExtractionResponse
        )

    async def _extract_artist_details(self,
                                      data_source: DataSource,
                                      existing_details: ArtistExistingDetails) -> ArtistDetailsExtractionResponse:
        if existing_details.about is None:
            extracted_details = self._build_invalid_extracted_details_response()
        else:
            extracted_details = await self._fetch_model_response(existing_details)

        return ArtistDetailsExtractionResponse(
            existing_details=existing_details,
            extracted_details=extracted_details,
            data_source=data_source
        )

    async def _fetch_model_response(self, existing_details: ArtistExistingDetails) -> ArtistExtractedDetails:
        prompt = """\
            Please return JSON describing the birth date, death date, and origin of music artists from this about \
            paragraph using the following schema:

            {
                "birth_date": Optional[DateDecision],
                "death_date": Optional[DateDecision],
                "origin": Optional[StringDecision],
                "gender": Optional[GenderDecision]
            }

            DateDecision = {"value": Optional[datetime], "evidence": Optional[str], "confidence": Optional[float]}
            StringDecision = {"value": Optional[str], "evidence": Optional[str], "confidence": Optional[float]}
            GenderDecision = {"value": Optional[Gender], "evidence": Optional[str], "confidence": Optional[float]}
            Gender = Enum of the following values: ["male", "female", "band", "unknown"] 

            All fields are optional. If you are not sure in your answer, simply return `null` instead.
            datetime fields should use the following format: `%Y-%m-%dT%H:%M:%s`.
            Confidence fields should return either a nullable of a float between 0-1.
            Evidence fields should return a simple string reasoning your decision.
            The `origin fields` should contain as much of the following details: country, state, county, city.
            In case the paragraph is discussing a band, treat the `birth_date` and `death_date` as if they were \
            describing formation date and disbandment date, respectively.

            Important: Only return a single piece of valid JSON text.

            Here is the about paragraph:
        """

        response = await self._model.generate_content_async(
            contents=dedent(prompt) + existing_details.about,
            generation_config={'response_mime_type': 'application/json'}
        )

        if response.parts:
            return self._serialize_response(response.text)

        logger.warning(
            f"Did not receive valid response parts for artist id `{existing_details.id}`. Returning empty details"
        )
        return self._build_invalid_extracted_details_response()

    @staticmethod
    def _build_invalid_extracted_details_response() -> ArtistExtractedDetails:
        return ArtistExtractedDetails(
            birth_date=None,
            death_date=None,
            origin=None,
            gender=None
        )

    @staticmethod
    def _serialize_response(response_text: str) -> ArtistExtractedDetails:
        try:
            return ArtistExtractedDetails.parse_raw(response_text)

        except ValidationError:
            logger.exception("Failed serializing response text. Returning empty details")
            return ArtistExtractedDetails(
                birth_date=None,
                death_date=None,
                origin=None,
                gender=None
            )
