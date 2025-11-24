from typing import List, Type

from genie_datastores.postgres.models import SpotifyFeaturedArtist

from data_collectors.logic.inserters.postgres.base_unique_database_inserter import (
    BaseUniqueDatabaseInserter,
)


class SpotifyFeaturedArtistsDatabaseInserter(BaseUniqueDatabaseInserter):
    @property
    def _unique_attributes(self) -> List[str]:
        return [SpotifyFeaturedArtist.track_id.key, SpotifyFeaturedArtist.artist_id.key]

    @property
    def _orm(self) -> Type[SpotifyFeaturedArtist]:
        return SpotifyFeaturedArtist
