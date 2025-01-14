from genie_datastores.mongo.operations import initialize_mongo
from genie_datastores.postgres.operations import get_database_engine

from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.inserters import ShazamArtistsDatabaseInserter
from data_collectors.logic.inserters.postgres import (
    ShazamTopTracksDatabaseInserter,
    ShazamTracksDatabaseInserter,
    ShazamArtistsPostgresDatabaseInserter,
    ChunksDatabaseInserter,
)


class ShazamInsertersComponentFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self._tools = tools

    @staticmethod
    def get_top_tracks_inserter(
        chunks_inserter: ChunksDatabaseInserter,
    ) -> ShazamTopTracksDatabaseInserter:
        return ShazamTopTracksDatabaseInserter(chunks_inserter)

    def get_tracks_inserter(self) -> ShazamTracksDatabaseInserter:
        return ShazamTracksDatabaseInserter(self._tools.get_database_engine())

    async def get_artists_inserter(self) -> ShazamArtistsDatabaseInserter:
        await initialize_mongo(self._tools.get_motor_client())
        return ShazamArtistsDatabaseInserter(
            postgres_inserter=self.get_artists_postgres_inserter(),
            pool_executor=self._tools.get_pool_executor(),
            db_engine=self._tools.get_database_engine(),
        )

    def get_artists_postgres_inserter(self) -> ShazamArtistsPostgresDatabaseInserter:
        return ShazamArtistsPostgresDatabaseInserter(self._tools.get_database_engine())
