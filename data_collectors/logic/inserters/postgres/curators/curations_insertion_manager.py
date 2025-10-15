from typing import List

from data_collectors.logic.inserters.postgres.curators.curated_tracks_database_inserter import (
    CuratedTracksDatabaseInserter,
)
from data_collectors.logic.inserters.postgres.curators.curators_collections_database_inserter import (
    CuratorsCollectionsDatabaseInserter,
)
from data_collectors.logic.inserters.postgres.curators.curators_database_inserter import CuratorsDatabaseInserter
from data_collectors.logic.models import Curation


class CurationsInsertionManager:
    def __init__(
        self,
        curators_inserter: CuratorsDatabaseInserter,
        curators_collections_inserter: CuratorsCollectionsDatabaseInserter,
        curated_tracks_inserter: CuratedTracksDatabaseInserter,
    ):
        self._curators_inserter = curators_inserter
        self._curators_collections_inserter = curators_collections_inserter
        self._curated_tracks_inserter = curated_tracks_inserter

    async def insert(self, curations: List[Curation]) -> None:
        await self._curators_inserter.insert(curations)
        await self._curators_collections_inserter.insert(curations)
        curated_tracks = [curation.to_track() for curation in curations]
        await self._curated_tracks_inserter.insert(curated_tracks)
