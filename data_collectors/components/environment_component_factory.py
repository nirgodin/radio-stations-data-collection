import os
from functools import lru_cache
from typing import List

from genie_common.utils import env_var_to_list


class EnvironmentComponentFactory:
    @staticmethod
    @lru_cache
    def get_musixmatch_api_key() -> str:
        return os.environ["MUSIXMATCH_API_KEY"]

    @staticmethod
    @lru_cache
    def get_genius_access_token() -> str:
        return os.environ["GENIUS_CLIENT_ACCESS_TOKEN"]

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
