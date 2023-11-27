import os
from ssl import create_default_context

from aiohttp import ClientSession, CookieJar, TCPConnector
from certifi import where
from spotipyio.logic.authentication.spotify_session import SpotifySession


class SessionsComponentFactory:
    @staticmethod
    def get_spotify_session() -> SpotifySession:
        return SpotifySession()

    @staticmethod
    def get_client_session() -> ClientSession:
        ssl_context = create_default_context(cafile=where())

        return ClientSession(
            connector=TCPConnector(ssl=ssl_context),
            cookie_jar=CookieJar(quote_cookie=False),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
        )

    @staticmethod
    def get_genius_session(bearer_token: str) -> ClientSession:
        ssl_context = create_default_context(cafile=where())

        return ClientSession(
            connector=TCPConnector(ssl=ssl_context),
            cookie_jar=CookieJar(quote_cookie=False),
            headers={
                "Accept": "application/json",
                "User-Agent": "CompuServe Classic/1.22",
                "Host": "api.genius.com",
                "Authorization": f"Bearer {bearer_token}"
            }
        )
