import os
from functools import lru_cache


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
