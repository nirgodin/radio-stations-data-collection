from aiohttp import ClientSession
from genie_datastores.milvus import MilvusClient
from genie_datastores.postgres.operations import get_database_engine
from spotipyio.logic.authentication.spotify_session import SpotifySession

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *


class MiscellaneousManagerFactory(BaseManagerFactory):
    def get_track_names_embeddings_manager(self,
                                           client_session: ClientSession,
                                           milvus_client: MilvusClient) -> TrackNamesEmbeddingsManager:
        pool_executor = self.tools.get_pool_executor()
        embeddings_collector = self.collectors.openai.get_track_names_embeddings_collector(
            pool_executor=pool_executor,
            session=client_session
        )

        return TrackNamesEmbeddingsManager(
            db_engine=get_database_engine(),
            embeddings_collector=embeddings_collector,
            milvus_client=milvus_client,
            db_updater=self.updaters.get_values_updater(pool_executor)
        )

    def get_radio_snapshots_manager(self, spotify_session: SpotifySession) -> RadioStationsSnapshotsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        chunks_generator = self.tools.get_chunks_generator()

        return RadioStationsSnapshotsManager(
            spotify_client=spotify_client,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            radio_tracks_database_inserter=self.inserters.get_radio_tracks_inserter(chunks_generator)
        )

    def get_primary_genre_manager(self) -> PrimaryGenreManager:
        pool_executor = self.tools.get_pool_executor()
        return PrimaryGenreManager(
            db_engine=get_database_engine(),
            genre_analyzer=self.analyzers.get_primary_genre_analyzer(),
            db_updater=self.updaters.get_values_updater(pool_executor)
        )

    def get_genres_mapping_manager(self) -> GenresMappingManager:
        chunks_generator = self.tools.get_chunks_generator()
        return GenresMappingManager(
            sheets_client=self.tools.get_google_sheets_client(),
            genres_inserter=self.inserters.get_genres_inserter(chunks_generator)
        )
