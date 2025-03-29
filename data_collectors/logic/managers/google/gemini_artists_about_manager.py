from datetime import datetime
from typing import Optional, List, Type, Any

from genie_common.tools import AioPoolExecutor, logger
from genie_datastores.postgres.models import Artist, Decision, Table
from genie_datastores.postgres.operations import insert_records
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import IManager
from data_collectors.logic.collectors import BaseArtistsExistingDetailsCollector
from data_collectors.logic.collectors import GeminiArtistsAboutParsingCollector
from data_collectors.logic.models import (
    DBUpdateRequest,
    ArtistDetailsExtractionResponse,
    BaseDecision,
    ArtistExistingDetails,
)
from data_collectors.logic.updaters import ValuesDatabaseUpdater


class GeminiArtistsAboutManager(IManager):
    def __init__(
        self,
        existing_details_collector: BaseArtistsExistingDetailsCollector,
        parsing_collector: GeminiArtistsAboutParsingCollector,
        pool_executor: AioPoolExecutor,
        db_engine: AsyncEngine,
        db_updater: ValuesDatabaseUpdater,
    ):
        self._existing_details_collector = existing_details_collector
        self._db_engine = db_engine
        self._parsing_collector = parsing_collector
        self._pool_executor = pool_executor
        self._db_updater = db_updater

    async def run(self, limit: Optional[int]) -> None:
        logger.info(f"Starting to run artists about manager for {limit} artists")
        artists_existing_details = await self._existing_details_collector.collect(limit)

        if artists_existing_details:
            await self._parse_artists_about(artists_existing_details)
        else:
            logger.info("Did not find any relevant artist about to parse. Aborting")

    async def _parse_artists_about(self, artists_existing_details: List[ArtistExistingDetails]) -> None:
        responses = await self._parsing_collector.collect(
            existing_details=artists_existing_details,
            data_source=self._existing_details_collector.data_source,
        )
        logger.info(f"Received {len(responses)} valid extraction responses. Updating artists database entries")
        await self._pool_executor.run(
            iterable=responses,
            func=self._update_artist_entries,
            expected_type=type(None),
        )

    async def _update_artist_entries(self, response: ArtistDetailsExtractionResponse) -> None:
        missing_fields = self._extract_missing_fields(response)

        if missing_fields:
            update_request = self._to_update_request(response, missing_fields)
            await self._db_updater.update_single(update_request)
            decisions = self._to_decisions(response, missing_fields)
            await insert_records(engine=self._db_engine, records=decisions)

        else:
            update_request = DBUpdateRequest(
                id=response.existing_details.id,
                values={Artist.update_date: datetime.utcnow()},
            )
            await self._db_updater.update_single(update_request)

    def _extract_missing_fields(self, response: ArtistDetailsExtractionResponse) -> List[Type[Artist]]:
        missing_fields = []

        for field in [
            Artist.birth_date,
            Artist.death_date,
            Artist.origin,
            Artist.gender,
        ]:
            existing_field = getattr(response.existing_details, field.key)
            extracted_field = getattr(response.extracted_details, field.key)

            if self._is_relevant_field(existing_field, extracted_field):
                missing_fields.append(field)

        return missing_fields

    @staticmethod
    def _is_relevant_field(existing_field: Optional[Any], extracted_field: Optional[BaseDecision]) -> bool:
        if existing_field is None:
            if extracted_field is not None:
                return extracted_field.value is not None

        return False

    @staticmethod
    def _to_update_request(
        response: ArtistDetailsExtractionResponse, missing_fields: List[Type[Artist]]
    ) -> DBUpdateRequest:
        values = {}

        for field in missing_fields:
            field_details = getattr(response.extracted_details, field.key)
            values[field] = field_details.value

        return DBUpdateRequest(id=response.existing_details.id, values=values)

    @staticmethod
    def _to_decisions(response: ArtistDetailsExtractionResponse, missing_fields: List[Type[Artist]]) -> List[Decision]:
        decisions = []

        for field in missing_fields:
            field_details: BaseDecision = getattr(response.extracted_details, field.key)
            decision = Decision(
                column=field.key,
                source=response.data_source,
                table=Table.ARTISTS,
                table_id=response.existing_details.id,
                confidence=field_details.confidence,
                evidence=field_details.evidence,
            )
            decisions.append(decision)

        return decisions
