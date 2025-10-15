from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.inserters.postgres import (
    ChunksDatabaseInserter,
    CuratorsDatabaseInserter,
    CuratorsCollectionsDatabaseInserter,
    CuratedTracksDatabaseInserter,
    CurationsInsertionManager,
)


class CurationsInsertersComponentFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self._tools = tools

    def get_curations_insertion_manager(self) -> CurationsInsertionManager:
        return CurationsInsertionManager(
            curators_inserter=self.get_curators_inserter(),
            curators_collections_inserter=self.get_curators_collections_inserter(),
            curated_tracks_inserter=self.get_curated_tracks_inserter(),
        )

    def get_curators_inserter(self) -> CuratorsDatabaseInserter:
        return CuratorsDatabaseInserter(db_engine=self._tools.get_database_engine())

    def get_curators_collections_inserter(self) -> CuratorsCollectionsDatabaseInserter:
        return CuratorsCollectionsDatabaseInserter(db_engine=self._tools.get_database_engine())

    def get_curated_tracks_inserter(self) -> CuratedTracksDatabaseInserter:
        return CuratedTracksDatabaseInserter(
            db_engine=self._tools.get_database_engine(), chunks_inserter=self._get_chunks_database_inserter()
        )

    def _get_chunks_database_inserter(self) -> ChunksDatabaseInserter:
        chunks_generator = self._tools.get_chunks_generator()
        return ChunksDatabaseInserter(
            db_engine=self._tools.get_database_engine(),
            chunks_generator=chunks_generator,
        )
