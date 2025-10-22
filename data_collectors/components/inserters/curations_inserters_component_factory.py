from sqlalchemy.ext.asyncio import AsyncEngine

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
        db_engine = self._tools.get_database_engine()
        chunks_inserter = self._tools.get_chunks_database_inserter()

        return CurationsInsertionManager(
            curators_inserter=self.get_curators_inserter(db_engine, chunks_inserter),
            curators_collections_inserter=self.get_curators_collections_inserter(db_engine, chunks_inserter),
            curated_tracks_inserter=self.get_curated_tracks_inserter(db_engine, chunks_inserter),
        )

    @staticmethod
    def get_curators_inserter(
        db_engine: AsyncEngine, chunks_inserter: ChunksDatabaseInserter
    ) -> CuratorsDatabaseInserter:
        return CuratorsDatabaseInserter(db_engine, chunks_inserter)

    @staticmethod
    def get_curators_collections_inserter(
        db_engine: AsyncEngine, chunks_inserter: ChunksDatabaseInserter
    ) -> CuratorsCollectionsDatabaseInserter:
        return CuratorsCollectionsDatabaseInserter(db_engine, chunks_inserter)

    @staticmethod
    def get_curated_tracks_inserter(
        db_engine: AsyncEngine, chunks_inserter: ChunksDatabaseInserter
    ) -> CuratedTracksDatabaseInserter:
        return CuratedTracksDatabaseInserter(db_engine, chunks_inserter)
