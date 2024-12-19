from typing import Optional

from genie_datastores.milvus import MilvusClient
from genie_datastores.mongo.operations import initialize_mongo

from data_collectors.components.inserters.billboard_inserters_component_factory import \
    BillboardInsertersComponentFactory
from data_collectors.components.inserters.shazam_inserters_compoent_factory import ShazamInsertersComponentFactory
from data_collectors.components.inserters.spotify_inserters_component_factory import SpotifyInsertersComponentFactory
from data_collectors.components.serializers.serializers_component_factory import SerializersComponentFactory
from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.inserters.milvus import MilvusChunksDatabaseInserter
from data_collectors.logic.inserters.mongo import MongoChunksDatabaseInserter, AboutParagraphsDatabaseInserter
from data_collectors.logic.inserters.postgres import RadioTracksDatabaseInserter, ChartEntriesDatabaseInserter, \
    ChunksDatabaseInserter, GenresDatabaseInserter


class InsertersComponentFactory:
    def __init__(self,
                 tools: ToolsComponentFactory,
                 billboard: Optional[BillboardInsertersComponentFactory] = None,
                 spotify: Optional[SpotifyInsertersComponentFactory] = None,
                 shazam: Optional[ShazamInsertersComponentFactory] = None,
                 serializers: Optional[SerializersComponentFactory] = None):
        self._tools = tools

        self.billboard = billboard or BillboardInsertersComponentFactory()
        self.spotify = spotify or SpotifyInsertersComponentFactory(tools)
        self.shazam = shazam or ShazamInsertersComponentFactory(tools)
        self._serializers = serializers or SerializersComponentFactory(tools)

    def get_radio_tracks_inserter(self) -> RadioTracksDatabaseInserter:
        return RadioTracksDatabaseInserter(
            db_engine=self._tools.get_database_engine(),
            chunks_inserter=self.get_chunks_database_inserter()
        )

    def get_chart_entries_inserter(self) -> ChartEntriesDatabaseInserter:
        return ChartEntriesDatabaseInserter(
            db_engine=self._tools.get_database_engine(),
            chunks_inserter=self.get_chunks_database_inserter()
        )

    def get_chunks_database_inserter(self) -> ChunksDatabaseInserter:
        chunks_generator = self._tools.get_chunks_generator()
        return ChunksDatabaseInserter(db_engine=self._tools.get_database_engine(), chunks_generator=chunks_generator)

    def get_genres_inserter(self) -> GenresDatabaseInserter:
        return GenresDatabaseInserter(
            db_engine=self._tools.get_database_engine(),
            chunks_inserter=self.get_chunks_database_inserter()
        )

    def get_milvus_chunks_inserter(self, milvus_client: MilvusClient) -> MilvusChunksDatabaseInserter:
        chunks_generator = self._tools.get_chunks_generator()
        return MilvusChunksDatabaseInserter(
            chunks_generator=chunks_generator,
            milvus_client=milvus_client
        )

    async def get_mongo_chunks_inserter(self) -> MongoChunksDatabaseInserter:
        await initialize_mongo()
        return MongoChunksDatabaseInserter(self._tools.get_chunks_generator())

    async def get_about_paragraphs_inserter(self) -> AboutParagraphsDatabaseInserter:
        chunks_inserter = await self.get_mongo_chunks_inserter()
        return AboutParagraphsDatabaseInserter(
            pool_executor=self._tools.get_pool_executor(),
            paragraphs_serializer=self._serializers.get_artists_about_paragraphs_serializer(),
            chunks_inserter=chunks_inserter,
        )
