from aiohttp import ClientSession
from genie_datastores.postgres.operations import get_database_engine
from spotipyio.logic.authentication.spotify_session import SpotifySession

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import (
    SpotifyPlaylistsArtistsManager,
    SpotifyPlaylistsTracksManager,
    ArtistsImagesGenderManager,
    SpotifyArtistsAboutManager
)


class SpotifyManagerFactory(BaseManagerFactory):
    def get_playlists_artists_manager(self, spotify_session: SpotifySession) -> SpotifyPlaylistsArtistsManager:
        return SpotifyPlaylistsArtistsManager(
            spotify_client=self.tools.get_spotify_client(spotify_session),
            db_updater=self.updaters.get_values_updater()
        )

    def get_playlists_tracks_manager(self, spotify_session: SpotifySession) -> SpotifyPlaylistsTracksManager:
        return SpotifyPlaylistsTracksManager(
            spotify_client=self.tools.get_spotify_client(spotify_session),
            db_updater=self.updaters.get_values_updater()
        )

    def get_artists_images_gender_manager(self,
                                          client_session: ClientSession,
                                          spotify_session: SpotifySession,
                                          confidence_threshold: float = 0.5) -> ArtistsImagesGenderManager:
        images_collector = self.collectors.spotify.get_artists_images_collector(
            client_session=client_session,
            spotify_client=self.tools.get_spotify_client(spotify_session)
        )

        return ArtistsImagesGenderManager(
            db_engine=get_database_engine(),
            artists_images_collector=images_collector,
            gender_detector=self.tools.get_image_gender_detector(confidence_threshold),
            db_updater=self.updaters.get_values_updater()
        )

    def get_artists_about_manager(self) -> SpotifyArtistsAboutManager:
        return SpotifyArtistsAboutManager(
            db_engine=get_database_engine(),
            abouts_collector=self.collectors.spotify.get_spotify_artists_about_collector()
        )
