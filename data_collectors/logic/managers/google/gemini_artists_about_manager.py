from datetime import datetime
from typing import Optional, List, Type, Any

from genie_common.tools import AioPoolExecutor, logger
from genie_datastores.postgres.models import SpotifyArtist, ShazamArtist, Artist, Decision, Table
from genie_datastores.postgres.operations import execute_query, insert_records
from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import IManager
from data_collectors.logic.collectors import GeminiArtistsAboutCollector
from data_collectors.logic.models import DBUpdateRequest, ArtistExistingDetails, \
    ArtistDetailsExtractionResponse, BaseDecision
from data_collectors.logic.updaters import ValuesDatabaseUpdater

ARTIST_ABOUT_COLUMNS = [
    SpotifyArtist.id,
    SpotifyArtist.about.label("spotify_about"),
    ShazamArtist.about.label("shazam_about"),
    Artist.origin,
    Artist.birth_date,
    Artist.death_date,
    Artist.gender
]


class GeminiArtistsAboutManager(IManager):
    def __init__(self,
                 db_engine: AsyncEngine,
                 artists_about_extractor: GeminiArtistsAboutCollector,
                 pool_executor: AioPoolExecutor,
                 db_updater: ValuesDatabaseUpdater):
        self._db_engine = db_engine
        self._artists_about_extractor = artists_about_extractor
        self._pool_executor = pool_executor
        self._db_updater = db_updater

    async def run(self, limit: Optional[int]) -> None:
        logger.info(f"Starting to run artists about manager for {limit} artists")
        artists_existing_details = await self._query_artists_about(limit)

        if artists_existing_details:
            responses = await self._artists_about_extractor.collect(artists_existing_details)
            logger.info(f"Received {len(responses)} valid extraction responses. Updating artists database entries")
            await self._pool_executor.run(
                iterable=responses,
                func=self._update_artist_entries,
                expected_type=type(None)
            )

    async def _query_artists_about(self, limit: Optional[int]) -> List[ArtistExistingDetails]:
        logger.info(f"Querying {limit} artists about")
        query = (
            select(ARTIST_ABOUT_COLUMNS)
            .where(SpotifyArtist.id == Artist.id)
            .where(Artist.shazam_id == ShazamArtist.id)
            .where(SpotifyArtist.id.isnot(None))
            .where(or_(SpotifyArtist.about.isnot(None), ShazamArtist.about.isnot(None)))
            .order_by(Artist.update_date.asc())
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)
        rows = query_result.all()

        return [ArtistExistingDetails.from_row(row) for row in rows]

    async def _update_artist_entries(self, response: ArtistDetailsExtractionResponse) -> None:
        missing_fields = self._extract_missing_fields(response)

        if missing_fields:
            update_request = self._to_update_request(response, missing_fields)
            await self._db_updater.update([update_request])
            decisions = self._to_decisions(response, missing_fields)
            await insert_records(engine=self._db_engine, records=decisions)

        else:
            update_request = DBUpdateRequest(
                id=response.existing_details.id,
                values={Artist.update_date: datetime.utcnow()}
            )
            await self._db_updater.update([update_request])

    def _extract_missing_fields(self, response: ArtistDetailsExtractionResponse) -> List[Type[Artist]]:
        missing_fields = []

        for field in [Artist.birth_date, Artist.death_date, Artist.origin, Artist.gender]:
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
    def _to_update_request(response: ArtistDetailsExtractionResponse, missing_fields: List[Type[Artist]]) -> DBUpdateRequest:
        values = {}

        for field in missing_fields:
            field_details = getattr(response.extracted_details, field.key)
            values[field] = field_details.value

        return DBUpdateRequest(
            id=response.existing_details.id,
            values=values
        )

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
                evidence=field_details.evidence
            )
            decisions.append(decision)

        return decisions
