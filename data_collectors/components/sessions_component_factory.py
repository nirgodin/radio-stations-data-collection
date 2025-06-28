from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from aiohttp import ClientSession, ClientResponseError
from genie_common.clients.utils import (
    create_client_session,
    build_authorization_headers,
)
from genie_datastores.redis.operations import get_redis
from playwright.async_api import Browser, async_playwright, Playwright, Error as PlaywrightError
from spotipyio.auth import SpotifyGrantType, SpotifySession, ClientCredentials
from spotipyio.extras.redis import RedisSessionCacheHandler
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from data_collectors.components.environment_component_factory import (
    EnvironmentComponentFactory,
)


class SessionsComponentFactory:
    def __init__(self, env: EnvironmentComponentFactory):
        self._env = env

    @asynccontextmanager
    async def enter_spotify_session(self) -> AsyncGenerator[SpotifySession, None]:
        session = None

        try:
            session = self.get_spotify_session()
            await self._start_spotify_session_with_retries(session)
            yield session

        except Exception as e:
            raise e

        finally:
            if session is not None:
                await session.stop()

    @asynccontextmanager
    async def enter_browser_session(self) -> AsyncGenerator[Browser, None]:
        async with async_playwright() as p:
            browser = await self._connect_to_browser_with_retries(p)
            yield browser

    def get_spotify_session(self) -> SpotifySession:
        credentials = self._env.get_spotify_credentials()
        token_request_url = self._env.get_spotify_token_request_url()

        return SpotifySession(token_request_url=token_request_url, credentials=credentials)

    @staticmethod
    def get_authorized_spotify_session() -> SpotifySession:
        cache_handler = RedisSessionCacheHandler(key="genie_radio_auth_v2", redis=get_redis())
        credentials = ClientCredentials(grant_type=SpotifyGrantType.AUTHORIZATION_CODE)

        return SpotifySession(session_cache_handler=cache_handler, credentials=credentials)

    @staticmethod
    def get_client_session() -> ClientSession:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        return create_client_session(headers)

    @staticmethod
    def get_genius_session(bearer_token: str) -> ClientSession:
        headers = {
            "Accept": "application/json",
            "User-Agent": "CompuServe Classic/1.22",
            "Host": "api.genius.com",
            "Authorization": f"Bearer {bearer_token}",
        }
        return create_client_session(headers)

    @staticmethod
    def get_openai_session(api_key: str) -> ClientSession:
        headers = build_authorization_headers(api_key)
        return create_client_session(headers)

    @staticmethod
    def get_google_geocoding_session(api_key: str) -> ClientSession:
        headers = {
            "X-RapidAPI-Key": api_key,
            "X-RapidAPI-Host": "google-maps-geocoding.p.rapidapi.com",
        }
        return create_client_session(headers)

    @retry(
        retry=retry_if_exception_type(ClientResponseError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    async def _start_spotify_session_with_retries(self, session: SpotifySession) -> None:
        await session.start()

    @retry(
        retry=retry_if_exception_type(PlaywrightError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    async def _connect_to_browser_with_retries(self, p: Playwright) -> Browser:
        return await p.chromium.connect(self._env.get_playwright_endpoint())
