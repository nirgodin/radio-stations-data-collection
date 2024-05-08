import os.path
from functools import lru_cache
from typing import Optional, List

from genie_common.tools import AioPoolExecutor, ChunksGenerator
from genie_common.utils import env_var_to_list
from genie_datastores.google.drive import GoogleDriveClient
from genie_datastores.google.sheets import GoogleSheetsClient, GoogleSheetsUploader, ShareSettings, PermissionType, Role
from genie_datastores.milvus import MilvusClient
from langid.langid import LanguageIdentifier, model
from shazamio import Shazam
from spotipyio import SpotifyClient
from spotipyio.logic.authentication.spotify_session import SpotifySession

from data_collectors.components.environment_component_factory import EnvironmentComponentFactory
from data_collectors.consts.image_gender_detector_consts import GENDER_MODEL_RESOURCES_DIR
from data_collectors.tools import ImageGenderDetector


class ToolsComponentFactory:
    def __init__(self, env: EnvironmentComponentFactory = EnvironmentComponentFactory()):
        self.env = env

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
    def get_google_drive_client() -> GoogleDriveClient:
        return GoogleDriveClient.create()

    @staticmethod
    @lru_cache
    def get_google_sheets_client() -> GoogleSheetsClient:
        return GoogleSheetsClient.create()

    def get_google_sheets_uploader(self) -> GoogleSheetsUploader:
        default_settings = {
            "share_settings": self._get_google_default_share_settings(),
            "folder_id": os.getenv("GOOGLE_SHEETS_DEFAULT_FOLDER_ID")
        }
        return GoogleSheetsUploader(
            google_sheets_client=self.get_google_sheets_client(),
            default_settings=default_settings
        )

    def get_image_gender_detector(self, confidence_threshold: float) -> ImageGenderDetector:
        if not os.path.exists(GENDER_MODEL_RESOURCES_DIR):
            gender_model_folder_id = self.env.get_gender_model_folder_id()
            os.mkdir(GENDER_MODEL_RESOURCES_DIR)
            drive_adapter = ToolsComponentFactory.get_google_drive_client()
            drive_adapter.download_all_dir_files(folder_id=gender_model_folder_id, local_dir=GENDER_MODEL_RESOURCES_DIR)

        return ImageGenderDetector.create(confidence_threshold)

    @staticmethod
    def get_chunks_generator(pool_executor: Optional[AioPoolExecutor] = None, chunk_size: int = 50) -> ChunksGenerator:
        executor = pool_executor or ToolsComponentFactory.get_pool_executor()
        return ChunksGenerator(
            pool_executor=executor,
            chunk_size=chunk_size
        )

    @staticmethod
    def get_language_identifier() -> LanguageIdentifier:
        return LanguageIdentifier.from_modelstring(model, norm_probs=True)

    def get_milvus_client(self) -> MilvusClient:
        uri = self.env.get_milvus_uri()
        token = self.env.get_milvus_token()

        return MilvusClient(uri=uri, token=token)

    def _get_google_default_share_settings(self) -> List[ShareSettings]:
        users = self.env.get_google_sheets_users()
        share_settings = []

        for user in users:
            user_setting = ShareSettings(
                email=user,
                permission_type=PermissionType.USER,
                role=Role.WRITER
            )
            share_settings.append(user_setting)

        return share_settings
