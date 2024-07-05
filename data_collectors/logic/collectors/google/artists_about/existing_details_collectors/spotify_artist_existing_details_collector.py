from typing import Optional, List

from genie_common.tools import logger
from genie_datastores.postgres.models import SpotifyArtist, Artist
from genie_datastores.models import DataSource
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select

from data_collectors.logic.collectors.google.artists_about.base_artist_existing_details_collector import (
    BaseArtistsExistingDetailsCollector
)
from data_collectors.logic.models import ArtistExistingDetails


ARTIST_ABOUT_COLUMNS = [
    SpotifyArtist.id,
    SpotifyArtist.about,
    Artist.origin,
    Artist.birth_date,
    Artist.death_date,
    Artist.gender
]


class SpotifyArtistsExistingDetailsCollector(BaseArtistsExistingDetailsCollector):
    async def collect(self, limit: Optional[int]) -> List[ArtistExistingDetails]:
        logger.info(f"Querying {limit} artists about")
        query = (
            select(ARTIST_ABOUT_COLUMNS)
            .where(SpotifyArtist.id == Artist.id)
            .where(SpotifyArtist.about.isnot(None))
            .order_by(Artist.update_date.asc())
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)
        rows = query_result.all()

        return [ArtistExistingDetails.from_row(row) for row in rows]

    @property
    def data_source(self) -> DataSource:
        return DataSource.SPOTIFY
