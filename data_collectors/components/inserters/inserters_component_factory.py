from postgres_client import get_database_engine

from data_collectors import RadioTracksDatabaseInserter
from data_collectors.components.inserters.billboard_inserters_component_factory import \
    BillboardInsertersComponentFactory
from data_collectors.components.inserters.spotify_inserters_component_factory import SpotifyInsertersComponentFactory


class InsertersComponentFactory:
    def __init__(self,
                 billboard: BillboardInsertersComponentFactory = BillboardInsertersComponentFactory(),
                 spotify: SpotifyInsertersComponentFactory = SpotifyInsertersComponentFactory()):
        self.billboard = billboard
        self.spotify = spotify

    @staticmethod
    def get_radio_tracks_inserter() -> RadioTracksDatabaseInserter:
        return RadioTracksDatabaseInserter(get_database_engine())
