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
    def get_radio_tracks_inserter() -> RadioTracksDatabaseInserter:
        return RadioTracksDatabaseInserter(get_database_engine())

    @staticmethod
    def get_chart_entries_inserter() -> ChartEntriesDatabaseInserter:
        return ChartEntriesDatabaseInserter(get_database_engine())
