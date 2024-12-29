import asyncio
from asyncio import AbstractEventLoop

from _pytest.fixtures import fixture
from fastapi import FastAPI
from genie_common.utils import random_alphanumeric_string
from genie_datastores.testing.postgres import PostgresTestkit, postgres_session
from spotipyio.auth import ClientCredentials, SpotifyGrantType
from spotipyio.logic.utils import random_client_credentials
from spotipyio.testing import SpotifyTestClient
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.app.utils import get_component_factory
from data_collectors.components import ComponentFactory
from data_collectors.components.environment_component_factory import (
    EnvironmentComponentFactory,
)
from main import app
from tests.tools.spotify_insertions_verifier import SpotifyInsertionsVerifier


@fixture(scope="session")
def event_loop() -> AbstractEventLoop:
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@fixture(scope="session")
def spotify_credentials() -> ClientCredentials:
    credentials = random_client_credentials()
    credentials.grant_type = SpotifyGrantType.CLIENT_CREDENTIALS

    return credentials


@fixture(scope="session")
async def spotify_test_client(
    spotify_credentials: ClientCredentials,
) -> SpotifyTestClient:
    async with SpotifyTestClient(credentials=spotify_credentials) as test_client:
        yield test_client


@fixture(scope="session")
def postgres_testkit() -> PostgresTestkit:
    with PostgresTestkit() as postgres_testkit:
        yield postgres_testkit


@fixture(scope="session")
def env_component_factory(
    spotify_credentials: ClientCredentials,
    spotify_test_client: SpotifyTestClient,
    postgres_testkit: PostgresTestkit,
) -> EnvironmentComponentFactory:
    default_env = {
        "SPOTIPY_CLIENT_ID": spotify_credentials.client_id,
        "SPOTIPY_CLIENT_SECRET": spotify_credentials.client_secret,
        "SPOTIPY_REDIRECT_URI": spotify_credentials.redirect_uri,
        "SPOTIPY_BASE_URL": spotify_test_client.get_base_url(),
        "SPOTIPY_TOKEN_REQUEST_URL": spotify_test_client._authorization_server.url_for(
            ""
        ).rstrip(
            "/"
        ),  # TODO: Externalize authorization server url
        "DATABASE_URL": postgres_testkit.get_database_url(),
        "EMAIL_USER": random_alphanumeric_string(),
        "EMAIL_PASSWORD": random_alphanumeric_string(),
    }
    return EnvironmentComponentFactory(default_env)


@fixture(scope="session")
def component_factory(
    env_component_factory: EnvironmentComponentFactory,
) -> ComponentFactory:
    return ComponentFactory(
        env=env_component_factory,
    )


@fixture(scope="session")
def test_client(component_factory: ComponentFactory) -> TestClient:
    app.dependency_overrides[get_component_factory] = lambda: component_factory
    yield TestClient(app)
    app.dependency_overrides = {}


@fixture(scope="function")
async def db_engine(postgres_testkit: PostgresTestkit) -> AsyncEngine:
    engine = postgres_testkit.get_database_engine()

    async with postgres_session(engine):
        yield engine


@fixture(scope="function")
def spotify_insertions_verifier(db_engine: AsyncEngine) -> SpotifyInsertionsVerifier:
    return SpotifyInsertionsVerifier(db_engine)
