from genie_datastores.postgres.operations import get_database_engine
from spotipyio import SpotifyClient

from data_collectors.logic.inserters.postgres import *


class SpotifyInsertersComponentFactory:
    @staticmethod
    def get_insertions_manager(spotify_client: SpotifyClient) -> SpotifyInsertionsManager:
        return SpotifyInsertionsManager(
            spotify_artists_inserter=SpotifyInsertersComponentFactory.get_spotify_artists_inserter(spotify_client),
            albums_inserter=SpotifyInsertersComponentFactory.get_albums_inserter(),
            spotify_tracks_inserter=SpotifyInsertersComponentFactory.get_spotify_tracks_inserter(),
            audio_features_inserter=SpotifyInsertersComponentFactory.get_audio_features_inserter(spotify_client),
            track_id_mapping_inserter=SpotifyInsertersComponentFactory.get_track_id_mapping_inserter(),
            artists_inserter=SpotifyInsertersComponentFactory.get_artists_inserter(),
            tracks_inserter=SpotifyInsertersComponentFactory.get_tracks_inserter(),
        )

    @staticmethod
    def get_spotify_artists_inserter(spotify_client: SpotifyClient) -> SpotifyArtistsDatabaseInserter:
        return SpotifyArtistsDatabaseInserter(spotify_client=spotify_client, db_engine=get_database_engine())

    @staticmethod
    def get_albums_inserter() -> SpotifyAlbumsDatabaseInserter:
        return SpotifyAlbumsDatabaseInserter(get_database_engine())

    @staticmethod
    def get_spotify_tracks_inserter() -> SpotifyTracksDatabaseInserter:
        return SpotifyTracksDatabaseInserter(get_database_engine())

    @staticmethod
    def get_audio_features_inserter(spotify_client: SpotifyClient) -> SpotifyAudioFeaturesDatabaseInserter:
        return SpotifyAudioFeaturesDatabaseInserter(spotify_client=spotify_client, db_engine=get_database_engine())

    @staticmethod
    def get_track_id_mapping_inserter() -> TrackIDMappingDatabaseInserter:
        return TrackIDMappingDatabaseInserter(get_database_engine())

    @staticmethod
    def get_artists_inserter() -> ArtistsDatabaseInserter:
        return ArtistsDatabaseInserter(get_database_engine())

    @staticmethod
    def get_tracks_inserter() -> TracksDatabaseInserter:
        return TracksDatabaseInserter(get_database_engine())
