from pytest_httpserver.httpserver import HandlerType

from tests.tools.test_clients.base_test_client import BaseTestClient


class WikipediaTestClient(BaseTestClient):
    def given_valid_response(self, title: str, response: str) -> None:
        request_handler = self._server.expect_request(
            uri=f"/{title}",
            method="GET",
            handler_type=HandlerType.ONESHOT,
        )
        request_handler.respond_with_data(response)
