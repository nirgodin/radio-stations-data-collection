from typing import List

from genie_datastores.models import DataSource
from genie_datastores.postgres.models import Artist, GeniusArtist, BaseORMModel
from sqlalchemy.sql import Select

from data_collectors.logic.collectors.google.artists_about.base_artist_existing_details_collector import (
    BaseArtistsExistingDetailsCollector,
)


class GeniusArtistsExistingDetailsCollector(BaseArtistsExistingDetailsCollector):
    @property
    def _source_specific_columns(self) -> List[BaseORMModel]:
        return [Artist.id.label("spotify_id"), GeniusArtist.id]

    def _add_source_specific_clauses(self, query: Select) -> Select:
        return (
            query.where(GeniusArtist.id == Artist.genius_id)
            .where(GeniusArtist.id.isnot(None))
            .where(Artist.id.isnot(None))
        )

    @property
    def data_source(self) -> DataSource:
        return DataSource.GENIUS
