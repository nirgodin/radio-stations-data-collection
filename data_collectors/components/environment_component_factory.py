import os
from functools import lru_cache


class EnvironmentComponentFactory:
    @staticmethod
    @lru_cache
    def get_musixmatch_api_key() -> str:
        return os.environ["MUSIXMATCH_API_KEY"]
