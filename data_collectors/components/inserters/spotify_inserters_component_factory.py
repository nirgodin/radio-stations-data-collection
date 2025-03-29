from spotipyio import SpotifyClient
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.inserters.postgres import *


class SpotifyInsertersComponentFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self._tools = tools

    def get_insertions_manager(self, spotify_client: SpotifyClient) -> SpotifyInsertionsManager:
        database_engine = self._tools.get_database_engine()
        return SpotifyInsertionsManager(
            spotify_artists_inserter=self.get_spotify_artists_inserter(spotify_client, database_engine),
            albums_inserter=self.get_albums_inserter(database_engine),
            spotify_tracks_inserter=self.get_spotify_tracks_inserter(database_engine),
            audio_features_inserter=self.get_audio_features_inserter(spotify_client, database_engine),
            track_id_mapping_inserter=self.get_track_id_mapping_inserter(database_engine),
            artists_inserter=self.get_artists_inserter(database_engine),
            tracks_inserter=self.get_tracks_inserter(database_engine),
            featured_artists_inserter=self.get_featured_artists_inserter(database_engine),
        )

    @staticmethod
    def get_spotify_artists_inserter(
        spotify_client: SpotifyClient, db_engine: AsyncEngine
    ) -> SpotifyArtistsDatabaseInserter:
        return SpotifyArtistsDatabaseInserter(spotify_client=spotify_client, db_engine=db_engine)

    @staticmethod
    def get_albums_inserter(db_engine: AsyncEngine) -> SpotifyAlbumsDatabaseInserter:
        return SpotifyAlbumsDatabaseInserter(db_engine)

    @staticmethod
    def get_spotify_tracks_inserter(
        db_engine: AsyncEngine,
    ) -> SpotifyTracksDatabaseInserter:
        return SpotifyTracksDatabaseInserter(db_engine)

    @staticmethod
    def get_audio_features_inserter(
        spotify_client: SpotifyClient, db_engine: AsyncEngine
    ) -> SpotifyAudioFeaturesDatabaseInserter:
        return SpotifyAudioFeaturesDatabaseInserter(spotify_client=spotify_client, db_engine=db_engine)

    @staticmethod
    def get_track_id_mapping_inserter(
        db_engine: AsyncEngine,
    ) -> TrackIDMappingDatabaseInserter:
        return TrackIDMappingDatabaseInserter(db_engine)

    @staticmethod
    def get_artists_inserter(db_engine: AsyncEngine) -> ArtistsDatabaseInserter:
        return ArtistsDatabaseInserter(db_engine)

    @staticmethod
    def get_tracks_inserter(db_engine: AsyncEngine) -> TracksDatabaseInserter:
        return TracksDatabaseInserter(db_engine)

    @staticmethod
    def get_featured_artists_inserter(
        db_engine: AsyncEngine,
    ) -> SpotifyFeaturedArtistsDatabaseInserter:
        return SpotifyFeaturedArtistsDatabaseInserter(db_engine)
