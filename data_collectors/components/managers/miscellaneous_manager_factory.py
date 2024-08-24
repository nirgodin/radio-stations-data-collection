from aiohttp import ClientSession
from genie_datastores.milvus import MilvusClient
from genie_datastores.postgres.models import TrackIDMapping
from genie_datastores.models import DataSource
from genie_datastores.postgres.operations import get_database_engine
from spotipyio import SpotifyClient
from spotipyio.logic.authentication.spotify_session import SpotifySession

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *
from data_collectors.logic.models import LyricsSourceDetails


class MiscellaneousManagerFactory(BaseManagerFactory):
    def get_track_names_embeddings_manager(self) -> TrackNamesEmbeddingsManager:
        embeddings_collector = self.collectors.openai.get_track_names_embeddings_collector()

        return TrackNamesEmbeddingsManager(
            db_engine=get_database_engine(),
            embeddings_collector=embeddings_collector,
            db_updater=self.updaters.get_values_updater()
        )

    def get_track_names_embeddings_retriever(self, milvus_client: MilvusClient) -> TrackNamesEmbeddingsRetrievalManager:
        return TrackNamesEmbeddingsRetrievalManager(
            db_engine=get_database_engine(),
            embeddings_retriever=self.collectors.openai.get_track_names_embeddings_retrieval_collector(),
            milvus_client=milvus_client,
            db_updater=self.updaters.get_values_updater()
        )

    def get_radio_snapshots_manager(self, spotify_session: SpotifySession) -> RadioStationsSnapshotsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)

        return RadioStationsSnapshotsManager(
            spotify_client=spotify_client,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            radio_tracks_database_inserter=self.inserters.get_radio_tracks_inserter()
        )

    def get_tracks_lyrics_manager(self, session: ClientSession) -> TracksLyricsManager:
        pool_executor = self.tools.get_pool_executor()
        musixmatch_collector = self.collectors.musixmatch.get_lyrics_collector(
            session=session,
            pool_executor=pool_executor,
            api_key=self.env.get_musixmatch_api_key()
        )
        prioritized_sources = [
            LyricsSourceDetails(
                column=TrackIDMapping.genius_id,
                collector=self.collectors.genius.get_lyrics_collector(session),
                data_source=DataSource.GENIUS
            ),
            LyricsSourceDetails(
                column=TrackIDMapping.shazam_id,
                collector=self.collectors.shazam.get_lyrics_collector(pool_executor),
                data_source=DataSource.SHAZAM
            ),
            LyricsSourceDetails(
                column=TrackIDMapping.musixmatch_id,
                collector=musixmatch_collector,
                data_source=DataSource.MUSIXMATCH
            )
        ]

        return TracksLyricsManager(
            db_engine=get_database_engine(),
            prioritized_sources=prioritized_sources,
            records_serializer=self.serializers.get_tracks_lyrics_serializer()
        )

    def get_lyrics_missing_ids_manager(self) -> TracksLyricsMissingIDsManager:
        return TracksLyricsMissingIDsManager(
            db_engine=get_database_engine(),
            chunks_inserter=self.inserters.get_chunks_database_inserter()
        )

    def get_tracks_vectorizer_manager(self, milvus_client: MilvusClient) -> TracksVectorizerManager:
        return TracksVectorizerManager(
            train_data_collector=self.collectors.misc.get_tracks_vectorizer_train_data_collector(),
            milvus_inserter=self.inserters.get_milvus_chunks_inserter(milvus_client),
            drive_folder_id=self.env.get_tracks_features_column_transformer_folder_id(),
            google_drive_client=self.tools.get_google_drive_client()
        )

    def get_release_radar_manager(self, spotify_authorized_session: SpotifySession) -> ReleaseRadarManager:
        return ReleaseRadarManager(
            db_engine=get_database_engine(),
            spotify_client=self.tools.get_spotify_client(spotify_authorized_session),
            playlist_id=self.env.get_release_radar_playlist_id()
        )
