from typing import Optional

from pytest_httpserver import HTTPServer


class BaseTestClient:
    def __init__(self, server: Optional[HTTPServer] = None):
        self._server = server

    def get_base_url(self) -> str:
        return self._server.url_for("").rstrip("/")

    def __enter__(self) -> "BaseTestClient":
        if self._server is None:
            self._server = HTTPServer()
            self._server.start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._server is not None:
            self._server.stop()
