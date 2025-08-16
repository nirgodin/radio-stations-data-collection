from pytest_httpserver import HTTPServer
from pytest_httpserver.httpserver import HandlerType

from tests.tools.playwright_container import PlaywrightContainer


class PlaywrightTestkit:
    def __init__(
        self, playwright_container: PlaywrightContainer = PlaywrightContainer(), server: HTTPServer = HTTPServer()
    ):
        self._playwright_container = playwright_container
        self._server = server

    def expect(self, uri: str, html: str) -> None:
        self._server.expect_request(
            uri=uri,
            method="GET",
            handler_type=HandlerType.ONESHOT,
        ).respond_with_data(html)

    def get_server_url(self) -> str:
        return self._server.url_for("").rstrip("/").replace("localhost", "host.docker.internal")

    def get_playwright_endpoint(self) -> str:
        host_port = self._playwright_container.get_exposed_port(self._playwright_container.port)
        return f"ws://127.0.0.1:{host_port}/"

    def __enter__(self) -> "PlaywrightTestkit":
        self._playwright_container.__enter__()
        self._server.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._playwright_container.__exit__(exc_type, exc_val, exc_tb)
        self._server.__exit__(exc_type, exc_val, exc_tb)
