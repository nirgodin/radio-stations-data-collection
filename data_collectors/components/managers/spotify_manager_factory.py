from aiohttp import ClientSession
from genie_datastores.postgres.operations import get_database_engine
from spotipyio.logic.authentication.spotify_session import SpotifySession

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *


class SpotifyManagerFactory(BaseManagerFactory):
    def get_playlists_artists_manager(self, spotify_session: SpotifySession) -> SpotifyPlaylistsArtistsManager:
        pool_executor = self.tools.get_pool_executor()
        return SpotifyPlaylistsArtistsManager(
            spotify_client=self.tools.get_spotify_client(spotify_session),
            db_updater=self.updaters.get_values_updater(pool_executor)
        )

    def get_playlists_tracks_manager(self, spotify_session: SpotifySession) -> SpotifyPlaylistsTracksManager:
        pool_executor = self.tools.get_pool_executor()
        return SpotifyPlaylistsTracksManager(
            spotify_client=self.tools.get_spotify_client(spotify_session),
            db_updater=self.updaters.get_values_updater(pool_executor)
        )

    def get_artists_images_gender_manager(self,
                                          client_session: ClientSession,
                                          spotify_session: SpotifySession,
                                          confidence_threshold: float = 0.5) -> ArtistsImagesGenderManager:
        pool_executor = self.tools.get_pool_executor()
        images_collector = self.collectors.spotify.get_artists_images_collector(
            client_session=client_session,
            spotify_client=self.tools.get_spotify_client(spotify_session),
            pool_executor=pool_executor
        )
        gender_model_folder_id = self.env.get_gender_model_folder_id()

        return ArtistsImagesGenderManager(
            db_engine=get_database_engine(),
            artists_images_collector=images_collector,
            gender_detector=self.tools.get_image_gender_detector(gender_model_folder_id, confidence_threshold),
            db_updater=self.updaters.get_values_updater(pool_executor)
        )
