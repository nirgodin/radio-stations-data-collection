from typing import Optional

from pytest_httpserver import HTTPServer
from pytest_httpserver.httpserver import HandlerType


class WikipediaTestClient:
    def __init__(self, server: Optional[HTTPServer] = None):
        self._server = server

    def get_base_url(self) -> str:
        return self._server.url_for("").rstrip("/")

    def given_valid_response(self, title: str, response: str) -> None:
        request_handler = self._server.expect_request(
            uri=f"/{title}",
            method="GET",
            handler_type=HandlerType.ONESHOT,
        )
        request_handler.respond_with_data(response)

    def __enter__(self) -> "WikipediaTestClient":
        if self._server is None:
            self._server = HTTPServer()
            self._server.start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._server is not None:
            self._server.stop()
