from typing import Optional, List

from genie_common.tools import logger
from genie_datastores.models import DataSource
from genie_datastores.mongo.models import AboutDocument
from genie_datastores.postgres.models import Artist, SpotifyArtist
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select, or_
from sqlalchemy.engine import Row, Result, ChunkedIteratorResult
from sqlalchemy.sql.elements import BinaryExpression

from data_collectors.logic.collectors.google.artists_about.base_artist_existing_details_collector import (
    BaseArtistsExistingDetailsCollector,
)
from data_collectors.logic.models import ArtistExistingDetails

ARTIST_INSIGHTS_COLUMNS = [
    Artist.origin,
    Artist.birth_date,
    Artist.death_date,
    Artist.gender,
]
ARTIST_ABOUT_COLUMNS = [
    Artist.id,
    SpotifyArtist.wikipedia_name,
    SpotifyArtist.wikipedia_language,
] + ARTIST_INSIGHTS_COLUMNS


class WikipediaArtistsExistingDetailsCollector(BaseArtistsExistingDetailsCollector):
    async def collect(self, limit: Optional[int]) -> List[ArtistExistingDetails]:
        logger.info(f"Querying {limit} artists wikipedia pages")
        query = (
            select(*ARTIST_ABOUT_COLUMNS)
            .where(SpotifyArtist.id == Artist.id)
            .where(SpotifyArtist.wikipedia_name.isnot(None))
            .where(self._is_any_artist_insight_missing())
            .order_by(Artist.update_date.asc())
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return await self._to_artists_details(query_result, limit)

    @staticmethod
    def _is_any_artist_insight_missing() -> BinaryExpression:
        conditions = [col.is_(None) for col in ARTIST_INSIGHTS_COLUMNS]
        return or_(*conditions)

    async def _to_artists_details(
        self, query_result: ChunkedIteratorResult, limit: Optional[int]
    ) -> List[ArtistExistingDetails]:
        artists_details = []

        for row in query_result:
            artist_existing_details = await self._collect_single_artist_summary(row)

            if artist_existing_details is not None:
                artists_details.append(artist_existing_details)

            if limit is not None and len(artists_details) >= limit:
                break

        return artists_details

    @staticmethod
    async def _collect_single_artist_summary(row: Row) -> ArtistExistingDetails:
        about_document = await AboutDocument.find_one(
            AboutDocument.entity_id == row.id,
            AboutDocument.source == DataSource.WIKIPEDIA,
        )

        if about_document is not None:
            return ArtistExistingDetails(
                id=row.id,
                about=about_document.about,
                origin=row.origin,
                birth_date=row.birth_date,
                death_date=row.death_date,
                gender=row.gender,
            )

    @property
    def data_source(self) -> DataSource:
        return DataSource.WIKIPEDIA
