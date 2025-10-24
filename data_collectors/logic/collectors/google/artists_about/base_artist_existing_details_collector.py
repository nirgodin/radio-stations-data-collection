from abc import ABC, abstractmethod
from typing import Optional, List

from genie_common.tools import logger
from genie_datastores.models import DataSource
from genie_datastores.mongo.models import AboutDocument
from genie_datastores.postgres.models import BaseORMModel, Artist
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select, or_
from sqlalchemy.engine import Result, Row
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import BinaryExpression

from data_collectors.contract import ICollector
from data_collectors.logic.models import ArtistExistingDetails


ARTIST_INSIGHTS_COLUMNS = [
    Artist.origin,
    Artist.birth_date,
    Artist.gender,
]


class BaseArtistsExistingDetailsCollector(ICollector, ABC):
    def __init__(self, db_engine: AsyncEngine):
        self._db_engine = db_engine

    async def collect(self, limit: Optional[int]) -> List[ArtistExistingDetails]:
        logger.info(f"Querying {limit} artists existing details for source `{self.data_source.value}`")
        query = self._build_query()
        query_result = await execute_query(engine=self._db_engine, query=query)

        return await self._to_artists_details(query_result, limit)

    def _build_query(self) -> Select:
        base_query = select(ARTIST_INSIGHTS_COLUMNS + [Artist.death_date] + self._source_specific_columns)
        query = self._add_source_specific_clauses(base_query)

        return query.where(self._is_any_artist_insight_missing()).order_by(Artist.update_date.asc())

    @property
    @abstractmethod
    def data_source(self) -> DataSource:
        raise NotImplementedError

    @property
    @abstractmethod
    def _source_specific_columns(self) -> List[BaseORMModel]:
        raise NotImplementedError

    @abstractmethod
    def _add_source_specific_clauses(self, query: Select) -> Select:
        raise NotImplementedError

    @staticmethod
    def _is_any_artist_insight_missing() -> BinaryExpression:
        conditions = [col.is_(None) for col in ARTIST_INSIGHTS_COLUMNS]
        return or_(*conditions)

    async def _to_artists_details(self, query_result: Result, limit: Optional[int]) -> List[ArtistExistingDetails]:
        artists_details = []

        for row in query_result:
            artist_existing_details = await self._collect_single_artist_summary(row)

            if artist_existing_details is not None:
                artists_details.append(artist_existing_details)

            if limit is not None and len(artists_details) >= limit:
                break

        return artists_details

    async def _collect_single_artist_summary(self, row: Row) -> ArtistExistingDetails:
        about_document = await AboutDocument.find_one(
            AboutDocument.entity_id == row.id,
            AboutDocument.source == self.data_source,
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
