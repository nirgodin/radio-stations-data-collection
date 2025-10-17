from aiohttp import ClientSession
from spotipyio import SpotifySession

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import JosieCurationsManager, SpotifyUserPlaylistsCurationsManager


class CurationsManagerFactory(BaseManagerFactory):
    def get_josie_curations_manager(
        self, spotify_session: SpotifySession, client_session: ClientSession
    ) -> JosieCurationsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        return JosieCurationsManager(
            josie_curations_collector=self.collectors.curations.get_josie_collector(client_session),
            spotify_client=spotify_client,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            curations_insertion_manager=self.inserters.curations.get_curations_insertion_manager(),
        )

    def get_spotify_user_playlists_curations_manager(
        self, spotify_session: SpotifySession
    ) -> SpotifyUserPlaylistsCurationsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        return SpotifyUserPlaylistsCurationsManager(
            spotify_client=spotify_client,
            db_engine=self.tools.get_database_engine(),
            spotify_playlists_curations_collector=self.collectors.curations.get_spotify_playlists_collector(
                spotify_client
            ),
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            curations_insertion_manager=self.inserters.curations.get_curations_insertion_manager(),
        )
