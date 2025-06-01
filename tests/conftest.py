import asyncio
from asyncio import AbstractEventLoop
from functools import partial

from aioresponses import aioresponses
from _pytest.fixtures import fixture
from genie_common.utils import random_alphanumeric_string
from genie_datastores.testing.mongo.mongo_testkit import MongoTestkit
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
from main import lifespan
from tests.testing_utils import app_test_client_session
from tests.tools.shazam_insertions_verifier import ShazamInsertionsVerifier
from tests.tools.spotify_insertions_verifier import SpotifyInsertionsVerifier
from tests.tools.wikipedia_test_client import WikipediaTestClient


@fixture
def event_loop() -> AbstractEventLoop:
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@fixture
def spotify_credentials() -> ClientCredentials:
    credentials = random_client_credentials()
    credentials.grant_type = SpotifyGrantType.CLIENT_CREDENTIALS

    return credentials


@fixture
async def spotify_test_client(
    spotify_credentials: ClientCredentials,
) -> SpotifyTestClient:
    async with SpotifyTestClient(credentials=spotify_credentials) as test_client:
        yield test_client


@fixture
def postgres_testkit() -> PostgresTestkit:
    with PostgresTestkit() as postgres_testkit:
        yield postgres_testkit


@fixture
def mongo_testkit() -> MongoTestkit:
    with MongoTestkit() as mongo_testkit:
        yield mongo_testkit


@fixture
def env_component_factory(
    spotify_credentials: ClientCredentials,
    spotify_test_client: SpotifyTestClient,
    postgres_testkit: PostgresTestkit,
    mongo_testkit: MongoTestkit,
    wikipedia_test_client: WikipediaTestClient,
) -> EnvironmentComponentFactory:
    # TODO: Externalize authorization server url
    token_request_url = spotify_test_client._authorization_server.url_for("")
    default_env = {
        "SPOTIPY_CLIENT_ID": spotify_credentials.client_id,
        "SPOTIPY_CLIENT_SECRET": spotify_credentials.client_secret,
        "SPOTIPY_REDIRECT_URI": spotify_credentials.redirect_uri,
        "SPOTIPY_BASE_URL": spotify_test_client.get_base_url(),
        "SPOTIPY_TOKEN_REQUEST_URL": token_request_url.rstrip("/"),
        "DATABASE_URL": postgres_testkit.get_database_url(),
        "EMAIL_USER": random_alphanumeric_string(),
        "EMAIL_PASSWORD": random_alphanumeric_string(),
        "MONGO_URI": mongo_testkit._container.get_connection_url(),
        "WIKIPEDIA_BASE_URL": wikipedia_test_client.get_base_url(),
    }
    return EnvironmentComponentFactory(default_env)


@fixture
def mock_aioresponses() -> aioresponses:
    with aioresponses() as mock_responses:
        yield mock_responses


@fixture
def component_factory(
    env_component_factory: EnvironmentComponentFactory,
) -> ComponentFactory:
    return ComponentFactory(
        env=env_component_factory,
    )


@fixture
def test_client(component_factory: ComponentFactory) -> TestClient:
    lifespan_context = partial(lifespan, component_factory=component_factory, jobs={})
    dependency_overrides = {get_component_factory: lambda: component_factory}

    with app_test_client_session(
        lifespan_context=lifespan_context, dependency_overrides=dependency_overrides
    ) as client:
        yield client


@fixture
async def db_engine(postgres_testkit: PostgresTestkit) -> AsyncEngine:
    engine = postgres_testkit.get_database_engine()

    async with postgres_session(engine):
        yield engine


@fixture
def spotify_insertions_verifier(db_engine: AsyncEngine) -> SpotifyInsertionsVerifier:
    return SpotifyInsertionsVerifier(db_engine)


@fixture
def shazam_insertions_verifier(db_engine: AsyncEngine) -> ShazamInsertionsVerifier:
    return ShazamInsertionsVerifier(db_engine)


@fixture
def wikipedia_test_client() -> WikipediaTestClient:
    with WikipediaTestClient() as test_client:
        yield test_client
