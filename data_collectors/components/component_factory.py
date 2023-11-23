from ssl import create_default_context

from aiohttp import ClientSession, TCPConnector, CookieJar
from certifi import where
from postgres_client import get_database_engine
from shazamio import Shazam
from spotipyio import AccessTokenGenerator, SpotifyClient
from spotipyio.logic.authentication.spotify_grant_type import SpotifyGrantType

from data_collectors import BillboardManager, RadioStationsSnapshotsManager, ShazamTopTracksManager, \
    ShazamInsertionsManager
from data_collectors.components.collectors import CollectorsComponentFactory
from data_collectors.components.inserters import InsertersComponentFactory
from data_collectors.tools import AioPoolExecutor


class ComponentFactory:
    def __init__(self,
                 collectors: CollectorsComponentFactory = CollectorsComponentFactory(),
                 inserters: InsertersComponentFactory = InsertersComponentFactory()):
        self.collectors = collectors
        self.inserters = inserters

    def get_shazam_top_tracks_manager(self) -> ShazamTopTracksManager:
        shazam = Shazam("EN")
        pool_executor = AioPoolExecutor()

        return ShazamTopTracksManager(
            top_tracks_collector=self.collectors.shazam.get_top_tracks_collector(shazam, pool_executor),
            insertions_manager=self.get_insertions_manager(shazam, pool_executor),
            top_tracks_inserter=self.inserters.shazam.get_top_tracks_inserter()
        )

    def get_insertions_manager(self, shazam: Shazam, pool_executor: AioPoolExecutor) -> ShazamInsertionsManager:
        return ShazamInsertionsManager(
            artists_collector=self.collectors.shazam.get_artists_collector(shazam, pool_executor),
            tracks_collector=self.collectors.shazam.get_tracks_collector(shazam, pool_executor),
            artists_inserter=self.inserters.shazam.get_artists_inserter(),
            tracks_inserter=self.inserters.shazam.get_tracks_inserter()
        )

    def get_billboard_manager(self, session: ClientSession) -> BillboardManager:
        spotify_client = self.get_spotify_client(session)
        return BillboardManager(
            db_engine=get_database_engine(),
            charts_collector=self.collectors.billboard.get_charts_collector(session),
            tracks_collector=self.collectors.billboard.get_tracks_collector(session, spotify_client),
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            tracks_inserter=self.inserters.billboard.get_tracks_inserter(),
            charts_inserter=self.inserters.billboard.get_charts_inserter(),
            tracks_updater=self.inserters.billboard.get_tracks_updater()
        )

    def get_radio_snapshots_manager(self, session: ClientSession) -> RadioStationsSnapshotsManager:
        spotify_client = self.get_spotify_client(session)
        return RadioStationsSnapshotsManager(
            spotify_client=spotify_client,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            radio_tracks_database_inserter=self.inserters.get_radio_tracks_inserter()
        )

    @staticmethod
    async def get_session() -> ClientSession:
        async with AccessTokenGenerator() as token_generator:
            response = await token_generator.generate(SpotifyGrantType.CLIENT_CREDENTIALS, None)

        access_token = response["access_token"]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}"
        }
        ssl_context = create_default_context(cafile=where())

        return ClientSession(
            headers=headers,  # TODO: Replace later with SpotifySession
            connector=TCPConnector(ssl=ssl_context),
            cookie_jar=CookieJar(quote_cookie=False)
        )

    @staticmethod
    def get_spotify_client(session: ClientSession) -> SpotifyClient:
        return SpotifyClient.create(session)
