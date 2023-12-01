import os.path
from functools import lru_cache

from genie_common.google import GoogleDriveAdapter
from genie_common.tools import AioPoolExecutor
from shazamio import Shazam
from spotipyio import SpotifyClient
from spotipyio.logic.authentication.spotify_session import SpotifySession

from data_collectors.consts.image_gender_detector_consts import GENDER_MODEL_RESOURCES_DIR
from data_collectors.tools import ImageGenderDetector


class ToolsComponentFactory:
    @staticmethod
    def get_pool_executor(pool_size: int = 5, validate_results: bool = True) -> AioPoolExecutor:
        return AioPoolExecutor(pool_size, validate_results)

    @staticmethod
    def get_shazam(language: str = "EN") -> Shazam:
        return Shazam(language)

    @staticmethod
    def get_spotify_client(spotify_session: SpotifySession) -> SpotifyClient:
        return SpotifyClient.create(spotify_session)

    @staticmethod
    @lru_cache
    def get_google_drive_adapter() -> GoogleDriveAdapter:
        return GoogleDriveAdapter.create()

    @staticmethod
    def get_image_gender_detector(gender_model_folder_id: str, confidence_threshold: float) -> ImageGenderDetector:
        if not os.path.exists(GENDER_MODEL_RESOURCES_DIR):
            os.mkdir(GENDER_MODEL_RESOURCES_DIR)
            drive_adapter = ToolsComponentFactory.get_google_drive_adapter()
            drive_adapter.download_all_dir_files(folder_id=gender_model_folder_id, local_dir=GENDER_MODEL_RESOURCES_DIR)

        return ImageGenderDetector.create(confidence_threshold)
