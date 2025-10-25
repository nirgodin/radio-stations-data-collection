from typing import List

from genie_datastores.models import DataSource
from genie_datastores.postgres.models import Artist, SpotifyArtist, BaseORMModel
from sqlalchemy.sql import Select

from data_collectors.logic.collectors.google.artists_about.base_artist_existing_details_collector import (
    BaseArtistsExistingDetailsCollector,
)


class WikipediaArtistsExistingDetailsCollector(BaseArtistsExistingDetailsCollector):
    @property
    def _source_specific_columns(self) -> List[BaseORMModel]:
        return [Artist.id, SpotifyArtist.wikipedia_name, SpotifyArtist.wikipedia_language]

    def _add_source_specific_clauses(self, query: Select) -> Select:
        return query.where(SpotifyArtist.id == Artist.id).where(SpotifyArtist.wikipedia_name.isnot(None))

    @property
    def data_source(self) -> DataSource:
        return DataSource.WIKIPEDIA
