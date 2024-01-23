from genie_common.tools import ChunksGenerator
from genie_datastores.postgres.operations import get_database_engine

from data_collectors.logic.inserters.postgres import RadioTracksDatabaseInserter, ChartEntriesDatabaseInserter
from data_collectors.components.inserters.billboard_inserters_component_factory import \
    BillboardInsertersComponentFactory
from data_collectors.components.inserters.shazam_inserters_compoent_factory import ShazamInsertersComponentFactory
from data_collectors.components.inserters.spotify_inserters_component_factory import SpotifyInsertersComponentFactory


class InsertersComponentFactory:
    def __init__(self,
                 billboard: BillboardInsertersComponentFactory = BillboardInsertersComponentFactory(),
                 spotify: SpotifyInsertersComponentFactory = SpotifyInsertersComponentFactory(),
                 shazam: ShazamInsertersComponentFactory = ShazamInsertersComponentFactory()):
        self.billboard = billboard
        self.spotify = spotify
        self.shazam = shazam

    @staticmethod
    def get_radio_tracks_inserter(chunks_generator: ChunksGenerator) -> RadioTracksDatabaseInserter:
        return RadioTracksDatabaseInserter(db_engine=get_database_engine(), chunks_generator=chunks_generator)

    @staticmethod
    def get_chart_entries_inserter(chunks_generator: ChunksGenerator) -> ChartEntriesDatabaseInserter:
        return ChartEntriesDatabaseInserter(db_engine=get_database_engine(), chunks_generator=chunks_generator)
