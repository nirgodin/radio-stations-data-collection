import os
from functools import lru_cache
from typing import List, Dict, Optional

from genie_common.utils import env_var_to_list
from spotipyio.auth import ClientCredentials

from data_collectors.tools import GoogleSearchConfig


class EnvironmentComponentFactory:
    def __init__(self, default_env: Optional[Dict[str, str]] = None):
        self._default_env = default_env or {}

    @lru_cache
    def get_spotify_credentials(self) -> ClientCredentials:
        return ClientCredentials(
            client_id=self._lookup_env_var("SPOTIPY_CLIENT_ID"),
            client_secret=self._lookup_env_var("SPOTIPY_CLIENT_SECRET"),
            redirect_uri=self._lookup_env_var("SPOTIPY_REDIRECT_URI"),
        )

    @lru_cache
    def get_mongo_uri(self) -> str:
        return self._lookup_env_var("MONGO_URI")

    @lru_cache
    def get_spotify_token_request_url(self) -> str:
        return self._lookup_env_var("SPOTIPY_TOKEN_REQUEST_URL")

    @lru_cache
    def get_spotify_base_url(self) -> str:
        return self._lookup_env_var("SPOTIPY_BASE_URL")

    @lru_cache
    def get_database_url(self) -> str:
        return self._lookup_env_var("DATABASE_URL")

    @lru_cache
    def get_email_user(self) -> str:
        return self._lookup_env_var("EMAIL_USER")

    @lru_cache
    def get_email_password(self) -> str:
        return self._lookup_env_var("EMAIL_PASSWORD")

    @staticmethod
    @lru_cache
    def get_musixmatch_api_key() -> str:
        return os.environ["MUSIXMATCH_API_KEY"]

    @lru_cache
    def get_genius_bearer_token(self) -> str:
        return self._lookup_env_var("GENIUS_BEARER_TOKEN")

    @staticmethod
    @lru_cache
    def get_openai_api_key() -> str:
        return os.environ["OPENAI_API_KEY"]

    @staticmethod
    @lru_cache
    def get_gender_model_folder_id() -> str:
        return os.environ["GENDER_MODEL_FOLDER_ID"]

    @staticmethod
    @lru_cache
    def get_tracks_features_column_transformer_folder_id() -> str:
        return os.environ["TRACKS_FEATURES_COLUMN_TRANSFORMER_FOLDER_ID"]

    @staticmethod
    @lru_cache
    def get_milvus_uri() -> str:
        return os.environ["MILVUS_URI"]

    @staticmethod
    @lru_cache
    def get_google_sheets_users() -> List[str]:
        return env_var_to_list("GOOGLE_SHEETS_USERS")

    @staticmethod
    @lru_cache
    def get_milvus_token() -> str:
        return os.environ["MILVUS_TOKEN"]

    @staticmethod
    @lru_cache
    def get_google_geocoding_api_key() -> str:
        return os.environ["GOOGLE_GEOCODING_API_KEY"]

    @lru_cache
    def get_gemini_api_key(self) -> str:
        return self._lookup_env_var("GEMINI_API_KEY")

    @staticmethod
    @lru_cache
    def get_release_radar_playlist_id() -> str:
        return os.environ["RELEASE_RADAR_PLAYLIST_ID"]

    def get_wikipedia_base_url(self) -> str:
        return self._lookup_env_var("WIKIPEDIA_BASE_URL", default="https://en.wikipedia.org/wiki")

    def get_playwright_endpoint(self) -> str:
        return self._lookup_env_var("PLAYWRIGHT_ENDPOINT", default="ws://127.0.0.1:3000/")

    def get_google_search_config(self) -> GoogleSearchConfig:
        return GoogleSearchConfig(
            base_url=self._lookup_env_var("GOOGLE_SEARCH_BASE_URL", "https://www.googleapis.com/customsearch/v1"),
            api_key=self._lookup_env_var("GOOGLE_SEARCH_API_KEY"),
            cx=self._lookup_env_var("GOOGLE_SEARCH_CX"),
        )

    def get_glz_base_url(self) -> str:
        return self._lookup_env_var("GLZ_BASE_URL", default="https://glz.co.il")

    def get_genius_api_base_url(self) -> str:
        return self._lookup_env_var("GENIUS_API_BASE_URL", default="https://api.genius.com")

    def get_genius_public_base_url(self) -> str:
        return self._lookup_env_var("GENIUS_PUBLIC_BASE_URL", default="https://genius.com/api")

    def get_rapid_api_key(self) -> str:
        return self._lookup_env_var("RAPID_API_KEY")

    def get_rapid_api_host(self) -> str:
        return self._lookup_env_var("RAPID_API_HOST", default="google-map-places.p.rapidapi.com")

    def get_rapid_base_url(self) -> str:
        return self._lookup_env_var("RAPID_BASE_URL", default="https://google-map-places.p.rapidapi.com")

    def get_billboard_base_url(self) -> str:
        return self._lookup_env_var("BILLBOARD_BASE_URL", default="https://www.billboard.com/charts")

    def get_josie_base_url(self) -> str:
        return self._lookup_env_var("JOSIE_BASE_URL", default="https://josieapp.com/api")

    def _lookup_env_var(self, key: str, default: Optional[str] = None) -> str:
        if key in self._default_env.keys():
            return self._default_env[key]

        if default is not None:
            return os.getenv(key, default)

        return os.environ[key]
