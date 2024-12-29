from typing import List, Optional, Tuple

from genie_common.utils import contains_any_hebrew_character
from genie_datastores.postgres.models import Artist, SpotifyArtist
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select

from data_collectors.logic.collectors.wikipedia.base_wikipedia_age_collector import (
    BaseWikipediaAgeCollector,
)


class WikipediaAgeNameCollector(BaseWikipediaAgeCollector):
    async def _get_missing_artists_details(
        self, limit: Optional[int]
    ) -> List[Tuple[str, str]]:
        query = (
            select(Artist.id, SpotifyArtist.name)
            .where(Artist.id == SpotifyArtist.id)
            .where(Artist.birth_date.is_(None))
            .where(Artist.death_date.is_(None))
            .order_by(Artist.update_date.desc())
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.all()

    def _get_artist_wikipedia_name(self, artist_detail: str) -> str:
        return artist_detail

    def _get_wikipedia_abbreviation(self, artist_detail: str) -> str:
        return "he" if contains_any_hebrew_character(artist_detail) else "en"
