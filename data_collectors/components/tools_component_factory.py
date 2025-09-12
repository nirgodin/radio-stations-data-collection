import os.path
from functools import lru_cache
from typing import Optional, List

from aiohttp import ClientSession
from async_lru import alru_cache
from genie_common.clients.google import GoogleTranslateClient
from genie_common.tools import AioPoolExecutor, ChunksGenerator, EmailSender
from genie_datastores.google.drive import GoogleDriveClient
from genie_datastores.google.sheets import (
    GoogleSheetsClient,
    GoogleSheetsUploader,
    ShareSettings,
    PermissionType,
    Role,
)
from genie_datastores.milvus import MilvusClient
from genie_datastores.mongo.operations import get_motor_client, initialize_mongo
from genie_datastores.postgres.operations import get_database_engine
from google import generativeai
from google.generativeai import GenerativeModel
from langid.langid import LanguageIdentifier, model
from openai import OpenAI
from shazamio import Shazam
from spotipyio import SpotifyClient
from spotipyio.auth import SpotifySession
from spotipyio.tools.matching import MultiEntityMatcher, EntityMatcher
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors import WikipediaTextCollector
from data_collectors.components.environment_component_factory import (
    EnvironmentComponentFactory,
)
from data_collectors.consts.image_gender_detector_consts import (
    GENDER_MODEL_RESOURCES_DIR,
)
from data_collectors.tools import (
    ImageGenderDetector,
    TranslationAdapter,
    GoogleSearchClient,
    GeniusClient,
    RapidAPIClient,
)


class ToolsComponentFactory:
    def __init__(self, env: EnvironmentComponentFactory):
        self._env = env

    def get_database_engine(self) -> AsyncEngine:
        url = self._env.get_database_url()
        return get_database_engine(url)

    @alru_cache
    async def initialize_mongo(self) -> None:
        motor_client = get_motor_client(uri=self._env.get_mongo_uri())
        await initialize_mongo(motor_client)

    def get_email_sender(self) -> EmailSender:
        return EmailSender(
            user=self._env.get_email_user(),
            password=self._env.get_email_password(),
        )

    @staticmethod
    def get_pool_executor(pool_size: int = 5, validate_results: bool = True) -> AioPoolExecutor:
        return AioPoolExecutor(pool_size, validate_results)

    @staticmethod
    def get_shazam(language: str = "EN") -> Shazam:
        return Shazam(language)

    def get_spotify_client(self, spotify_session: SpotifySession) -> SpotifyClient:
        return SpotifyClient.create(session=spotify_session, base_url=self._env.get_spotify_base_url())

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
            "folder_id": os.getenv("GOOGLE_SHEETS_DEFAULT_FOLDER_ID"),
        }
        return GoogleSheetsUploader(
            google_sheets_client=self.get_google_sheets_client(),
            default_settings=default_settings,
        )

    def get_image_gender_detector(self, confidence_threshold: float) -> ImageGenderDetector:
        if not os.path.exists(GENDER_MODEL_RESOURCES_DIR):
            gender_model_folder_id = self._env.get_gender_model_folder_id()
            os.mkdir(GENDER_MODEL_RESOURCES_DIR)
            drive_adapter = ToolsComponentFactory.get_google_drive_client()
            drive_adapter.download_all_dir_files(folder_id=gender_model_folder_id, local_dir=GENDER_MODEL_RESOURCES_DIR)

        return ImageGenderDetector.create(confidence_threshold)

    @staticmethod
    def get_chunks_generator(pool_executor: Optional[AioPoolExecutor] = None, chunk_size: int = 50) -> ChunksGenerator:
        executor = pool_executor or ToolsComponentFactory.get_pool_executor()
        return ChunksGenerator(pool_executor=executor, chunk_size=chunk_size)

    @staticmethod
    def get_language_identifier() -> LanguageIdentifier:
        return LanguageIdentifier.from_modelstring(model, norm_probs=True)

    def get_milvus_client(self) -> MilvusClient:
        uri = self._env.get_milvus_uri()
        token = self._env.get_milvus_token()

        return MilvusClient(uri=uri, token=token)

    def get_openai(self) -> OpenAI:
        return OpenAI(api_key=self._env.get_openai_api_key())

    @staticmethod
    def get_google_translate_client() -> GoogleTranslateClient:
        return GoogleTranslateClient.create()

    def get_translation_adapter(self) -> TranslationAdapter:
        return TranslationAdapter(
            translation_client=self.get_google_translate_client(),
            db_engine=get_database_engine(),
        )

    def get_gemini_model(self, model_name: str = "models/gemini-1.5-pro-latest") -> GenerativeModel:
        generativeai.configure(api_key=self._env.get_gemini_api_key())
        return GenerativeModel(model_name=model_name)

    @staticmethod
    def get_multi_entity_matcher(
        entity_matcher: Optional[EntityMatcher] = None,
    ) -> MultiEntityMatcher:
        return MultiEntityMatcher(entity_matcher or EntityMatcher())

    def get_wikipedia_text_collector(self, session: ClientSession) -> WikipediaTextCollector:
        return WikipediaTextCollector(session=session, base_url=self._env.get_wikipedia_base_url())

    def get_google_search_client(self, session: ClientSession) -> GoogleSearchClient:
        return GoogleSearchClient(
            config=self._env.get_google_search_config(), session=session, pool_executor=self.get_pool_executor()
        )

    def get_genius_client(self, session: ClientSession) -> GeniusClient:
        return GeniusClient(
            session=session,
            api_base_url=self._env.get_genius_api_base_url(),
            public_base_url=self._env.get_genius_public_base_url(),
            bearer_token=self._env.get_genius_bearer_token(),
        )

    def get_rapid_api_client(self, session: ClientSession) -> RapidAPIClient:
        return RapidAPIClient(
            session=session,
            api_key=self._env.get_rapid_api_key(),
            api_host=self._env.get_rapid_api_host(),
            base_url=self._env.get_rapid_base_url(),
        )

    def _get_google_default_share_settings(self) -> List[ShareSettings]:
        users = self._env.get_google_sheets_users()
        share_settings = []

        for user in users:
            user_setting = ShareSettings(email=user, permission_type=PermissionType.USER, role=Role.WRITER)
            share_settings.append(user_setting)

        return share_settings
