from spotipyio import SpotifyClient
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.inserters.postgres import *


class SpotifyInsertersComponentFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self._tools = tools

    def get_insertions_manager(self, spotify_client: SpotifyClient) -> SpotifyInsertionsManager:
        database_engine = self._tools.get_database_engine()
        chunks_inserter = self._tools.get_chunks_database_inserter()

        return SpotifyInsertionsManager(
            spotify_artists_inserter=self.get_spotify_artists_inserter(
                spotify_client, database_engine, chunks_inserter
            ),
            albums_inserter=self.get_albums_inserter(database_engine, chunks_inserter),
            spotify_tracks_inserter=self.get_spotify_tracks_inserter(database_engine, chunks_inserter),
            audio_features_inserter=self.get_audio_features_inserter(spotify_client, database_engine, chunks_inserter),
            track_id_mapping_inserter=self.get_track_id_mapping_inserter(database_engine, chunks_inserter),
            artists_inserter=self.get_artists_inserter(database_engine, chunks_inserter),
            tracks_inserter=self.get_tracks_inserter(database_engine, chunks_inserter),
            featured_artists_inserter=self.get_featured_artists_inserter(database_engine, chunks_inserter),
        )

    @staticmethod
    def get_spotify_artists_inserter(
        spotify_client: SpotifyClient,
        db_engine: AsyncEngine,
        chunks_inserter: ChunksDatabaseInserter,
    ) -> SpotifyArtistsDatabaseInserter:
        return SpotifyArtistsDatabaseInserter(
            db_engine=db_engine, chunks_inserter=chunks_inserter, spotify_client=spotify_client
        )

    @staticmethod
    def get_albums_inserter(
        db_engine: AsyncEngine, chunks_inserter: ChunksDatabaseInserter
    ) -> SpotifyAlbumsDatabaseInserter:
        return SpotifyAlbumsDatabaseInserter(db_engine, chunks_inserter)

    @staticmethod
    def get_spotify_tracks_inserter(
        db_engine: AsyncEngine,
        chunks_inserter: ChunksDatabaseInserter,
    ) -> SpotifyTracksDatabaseInserter:
        return SpotifyTracksDatabaseInserter(db_engine, chunks_inserter)

    @staticmethod
    def get_audio_features_inserter(
        spotify_client: SpotifyClient,
        db_engine: AsyncEngine,
        chunks_inserter: ChunksDatabaseInserter,
    ) -> SpotifyAudioFeaturesDatabaseInserter:
        return SpotifyAudioFeaturesDatabaseInserter(
            db_engine=db_engine,
            chunks_inserter=chunks_inserter,
            spotify_client=spotify_client,
        )

    @staticmethod
    def get_track_id_mapping_inserter(
        db_engine: AsyncEngine,
        chunks_inserter: ChunksDatabaseInserter,
    ) -> TrackIDMappingDatabaseInserter:
        return TrackIDMappingDatabaseInserter(db_engine, chunks_inserter)

    @staticmethod
    def get_artists_inserter(
        db_engine: AsyncEngine, chunks_inserter: ChunksDatabaseInserter
    ) -> ArtistsDatabaseInserter:
        return ArtistsDatabaseInserter(db_engine, chunks_inserter)

    @staticmethod
    def get_tracks_inserter(db_engine: AsyncEngine, chunks_inserter: ChunksDatabaseInserter) -> TracksDatabaseInserter:
        return TracksDatabaseInserter(db_engine, chunks_inserter)

    @staticmethod
    def get_featured_artists_inserter(
        db_engine: AsyncEngine,
        chunks_inserter: ChunksDatabaseInserter,
    ) -> SpotifyFeaturedArtistsDatabaseInserter:
        return SpotifyFeaturedArtistsDatabaseInserter(db_engine, chunks_inserter)
