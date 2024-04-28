from aiohttp import ClientSession
from genie_common.clients.utils import create_client_session, build_authorization_headers
from spotipyio.logic.authentication.spotify_session import SpotifySession


class SessionsComponentFactory:
    @staticmethod
    def get_spotify_session() -> SpotifySession:
        return SpotifySession()

    @staticmethod
    def get_client_session() -> ClientSession:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        return create_client_session(headers)

    @staticmethod
    def get_genius_session(bearer_token: str) -> ClientSession:
        headers = {
            "Accept": "application/json",
            "User-Agent": "CompuServe Classic/1.22",
            "Host": "api.genius.com",
            "Authorization": f"Bearer {bearer_token}"
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
            "X-RapidAPI-Host": "google-maps-geocoding.p.rapidapi.com"
        }
        return create_client_session(headers)
