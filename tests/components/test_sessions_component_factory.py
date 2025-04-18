from functools import partial
from http import HTTPStatus
from unittest.mock import AsyncMock, patch

import pytest
from _pytest.fixtures import fixture
from aiohttp import ClientResponseError, RequestInfo
from genie_common.utils import random_enum_value
from multidict import CIMultiDictProxy, CIMultiDict
from spotipyio import SpotifySession
from spotipyio.logic.utils import random_alphanumeric_string
from yarl import URL

from data_collectors.components import ComponentFactory
from tests.testing_utils import raise_exception


class TestSessionsComponentFactory:
    async def test_enter_spotify_session__valid_response__returns_active_session(
        self, component_factory: ComponentFactory, mock_spotify_session_start: AsyncMock
    ):
        async with component_factory.sessions.enter_spotify_session() as session:
            assert isinstance(session, SpotifySession)

    async def test_enter_spotify_session__client_response_error_on_all_retries__reraise_client_response_error(
        self, component_factory: ComponentFactory, mock_spotify_session_start: AsyncMock
    ):
        mock_spotify_session_start.side_effect = partial(raise_exception, self._a_random_client_response_error())

        with pytest.raises(ClientResponseError):
            async with component_factory.sessions.enter_spotify_session():
                pass

        assert mock_spotify_session_start.call_count == 3

    async def test_enter_spotify_session__non_client_response_error__reraise_without_retries(
        self, component_factory: ComponentFactory, mock_spotify_session_start: AsyncMock
    ):
        mock_spotify_session_start.side_effect = raise_exception

        with pytest.raises(Exception):
            async with component_factory.sessions.enter_spotify_session():
                pass

        assert mock_spotify_session_start.call_count == 1

    @fixture
    def mock_spotify_session_start(self) -> AsyncMock:
        with patch.object(SpotifySession, "start", new_callable=AsyncMock) as start_session:
            yield start_session

    @staticmethod
    def _a_random_client_response_error() -> ClientResponseError:
        headers = CIMultiDictProxy(CIMultiDict())
        url = random_alphanumeric_string()
        request_info = RequestInfo(url=URL(url), method="POST", headers=headers, real_url=URL(url))

        return ClientResponseError(
            request_info=request_info,
            history=(),
            status=random_enum_value(HTTPStatus).value,
            message=random_alphanumeric_string(),
            headers=headers,
        )
