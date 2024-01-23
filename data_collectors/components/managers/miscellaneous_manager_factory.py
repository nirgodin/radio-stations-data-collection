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
            spotify_tracks_updater=self.updaters.get_spotify_tracks_updater(pool_executor)
        )

    def get_radio_snapshots_manager(self, spotify_session: SpotifySession) -> RadioStationsSnapshotsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        chunks_generator = self.tools.get_chunks_generator()

        return RadioStationsSnapshotsManager(
            spotify_client=spotify_client,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            radio_tracks_database_inserter=self.inserters.get_radio_tracks_inserter(chunks_generator)
        )
