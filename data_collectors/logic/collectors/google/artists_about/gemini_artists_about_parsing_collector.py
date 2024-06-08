from functools import partial
from textwrap import dedent
from textwrap import dedent
from typing import Any, List

from genie_common.tools import AioPoolExecutor, logger
from genie_datastores.postgres.models import DataSource
from google.generativeai import GenerativeModel

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
        extracted_details = ArtistExtractedDetails.parse_raw(response.text)

        return ArtistDetailsExtractionResponse(
            existing_details=existing_details,
            extracted_details=extracted_details,
            data_source=data_source
        )
