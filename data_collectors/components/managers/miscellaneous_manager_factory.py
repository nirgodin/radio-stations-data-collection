from aiohttp import ClientSession
from genie_datastores.milvus import MilvusClient
from genie_datastores.postgres.models import TrackIDMapping, DataSource
from genie_datastores.postgres.operations import get_database_engine
from spotipyio.logic.authentication.spotify_session import SpotifySession

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *
from data_collectors.logic.models import LyricsSourceDetails


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
            db_updater=self.updaters.get_values_updater()
        )

    def get_radio_snapshots_manager(self, spotify_session: SpotifySession) -> RadioStationsSnapshotsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        chunks_generator = self.tools.get_chunks_generator()

        return RadioStationsSnapshotsManager(
            spotify_client=spotify_client,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            radio_tracks_database_inserter=self.inserters.get_radio_tracks_inserter(chunks_generator)
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
                collector=self.collectors.genius.get_lyrics_collector(session, pool_executor),
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
        chunks_generator = self.tools.get_chunks_generator()
        return TracksLyricsMissingIDsManager(
            db_engine=get_database_engine(),
            chunks_inserter=self.inserters.get_chunks_database_inserter(chunks_generator)
        )
