from typing import List, Optional, Tuple

from genie_datastores.postgres.models import Artist, SpotifyArtist, Gender
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.engine import Row

from data_collectors.consts.wikipedia_consts import WIKIPEDIA_NAME_LANGUAGE_SEPARATOR
from data_collectors.logic.collectors.wikipedia.base_wikipedia_age_collector import (
    BaseWikipediaAgeCollector,
)


class WikipediaAgeLinkCollector(BaseWikipediaAgeCollector):
    async def _get_missing_artists_details(
        self, limit: Optional[int]
    ) -> List[Tuple[str, str]]:
        query = (
            select(
                SpotifyArtist.id,
                SpotifyArtist.wikipedia_name,
                SpotifyArtist.wikipedia_language,
            )
            .where(Artist.id == SpotifyArtist.id)
            .where(Artist.birth_date.is_(None))
            .where(Artist.death_date.is_(None))
            .where(Artist.gender.notin_([Gender.BAND]))
            .where(SpotifyArtist.wikipedia_name.isnot(None))
            .where(SpotifyArtist.wikipedia_language.isnot(None))
            .order_by(Artist.update_date.desc())
            .limit(limit)
        )
        query_response = await execute_query(engine=self._db_engine, query=query)
        query_result = query_response.all()

        return [self._serialize_row_to_details(row) for row in query_result]

    def _get_artist_wikipedia_name(self, artist_detail: str) -> str:
        return artist_detail.split(WIKIPEDIA_NAME_LANGUAGE_SEPARATOR)[0]

    def _get_wikipedia_abbreviation(self, artist_detail: str) -> str:
        return artist_detail.split(WIKIPEDIA_NAME_LANGUAGE_SEPARATOR)[1]

    @staticmethod
    def _serialize_row_to_details(row: Row) -> Tuple[str, str]:
        details = f"{row.wikipedia_name}{WIKIPEDIA_NAME_LANGUAGE_SEPARATOR}{row.wikipedia_language}"
        return row.id, details
