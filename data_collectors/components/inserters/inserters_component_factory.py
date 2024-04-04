from genie_common.tools import ChunksGenerator
from genie_datastores.postgres.operations import get_database_engine

from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.inserters.postgres import RadioTracksDatabaseInserter, ChartEntriesDatabaseInserter, \
    ChunksDatabaseInserter, GenresDatabaseInserter
from data_collectors.components.inserters.billboard_inserters_component_factory import \
    BillboardInsertersComponentFactory
from data_collectors.components.inserters.shazam_inserters_compoent_factory import ShazamInsertersComponentFactory
from data_collectors.components.inserters.spotify_inserters_component_factory import SpotifyInsertersComponentFactory


class InsertersComponentFactory:
    def __init__(self,
                 billboard: BillboardInsertersComponentFactory = BillboardInsertersComponentFactory(),
                 spotify: SpotifyInsertersComponentFactory = SpotifyInsertersComponentFactory(),
                 shazam: ShazamInsertersComponentFactory = ShazamInsertersComponentFactory(),
                 tools: ToolsComponentFactory = ToolsComponentFactory()):
        self.billboard = billboard
        self.spotify = spotify
        self.shazam = shazam
        self._tools = tools

    def get_radio_tracks_inserter(self) -> RadioTracksDatabaseInserter:
        return RadioTracksDatabaseInserter(
            db_engine=get_database_engine(),
            chunks_inserter=self.get_chunks_database_inserter()
        )

    def get_chart_entries_inserter(self) -> ChartEntriesDatabaseInserter:
        return ChartEntriesDatabaseInserter(
            db_engine=get_database_engine(),
            chunks_inserter=self.get_chunks_database_inserter()
        )

    def get_chunks_database_inserter(self) -> ChunksDatabaseInserter:
        chunks_generator = self._tools.get_chunks_generator()
        return ChunksDatabaseInserter(db_engine=get_database_engine(), chunks_generator=chunks_generator)

    def get_genres_inserter(self) -> GenresDatabaseInserter:
        return GenresDatabaseInserter(
            db_engine=get_database_engine(),
            chunks_inserter=self.get_chunks_database_inserter()
        )
