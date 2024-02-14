from typing import Dict

from genie_common.tools import logger
from genie_datastores.postgres.models import PrimaryGenre, Genre
from genie_datastores.postgres.operations import execute_query, get_database_engine
from sqlalchemy import select

from data_collectors.logic.analyzers import *


class AnalyzersComponentFactory:
    @staticmethod
    def get_wikipedia_age_analyzer() -> WikipediaAgeAnalyzer:
        return WikipediaAgeAnalyzer()

    @staticmethod
    async def get_primary_genre_analyzer() -> PrimaryGenreAnalyzer:
        genres_mapper = await AnalyzersComponentFactory.get_genre_mapper_analyzer()
        return PrimaryGenreAnalyzer(genres_mapper)

    @staticmethod
    async def get_genre_mapper_analyzer() -> GenreMapperAnalyzer:
        genres_mapping = await AnalyzersComponentFactory._query_genres_mapping()
        return GenreMapperAnalyzer(genres_mapping)

    @staticmethod
    async def _query_genres_mapping() -> Dict[str, PrimaryGenre]:
        logger.info("Querying database for genres to primary genres mapping")
        db_engine = get_database_engine()
        query = (
            select(Genre.id, Genre.primary_genre)
            .distinct(Genre.id)
        )
        query_result = await execute_query(engine=db_engine, query=query)
        rows = query_result.all()

        return {row.id: row.primary_genre for row in rows}
