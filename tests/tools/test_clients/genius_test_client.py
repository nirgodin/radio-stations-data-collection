from typing import Any, Dict

from pytest_httpserver.httpserver import HandlerType

from tests.tools.test_clients.base_test_client import BaseTestClient


class GeniusTestClient(BaseTestClient):
    def expect_search_artist_request(self, name: str, response: Dict[str, Any]) -> None:
        request_handler = self._server.expect_request(
            uri="/search/artist",
            method="GET",
            query_string={"q": name},
            handler_type=HandlerType.ONESHOT,
        )
        request_handler.respond_with_json(response)
