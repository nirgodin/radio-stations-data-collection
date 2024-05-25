from typing import Dict, Type, TypeVar

from aiohttp import ClientSession
from genie_datastores.postgres.models import Chart
from genie_datastores.postgres.operations import get_database_engine
from spotipyio.logic.authentication.spotify_session import SpotifySession

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *

TranslationManager = TypeVar("TranslationManager", bound=BaseTranslationManager)


class TranslationManagerFactory(BaseManagerFactory):
    def get_spotify_israeli_artists_translation_manager(self) -> SpotifyIsraeliArtistsTranslationManager:
        return self._create_translation_manager(SpotifyIsraeliArtistsTranslationManager)

    def get_spotify_israeli_tracks_translation_manager(self) -> SpotifyIsraeliTracksTranslationManager:
        return self._create_translation_manager(SpotifyIsraeliTracksTranslationManager)

    def get_shazam_israeli_artists_translation_manager(self) -> ShazamIsraeliArtistsTranslationManager:
        return self._create_translation_manager(ShazamIsraeliArtistsTranslationManager)

    def _create_translation_manager(self, manager: Type[BaseTranslationManager]) -> TranslationManager:
        return manager(
            db_engine=get_database_engine(),
            pool_executor=self.tools.get_pool_executor(),
            translation_client=self.tools.get_google_translate_client(),
            chunks_inserter=self.inserters.get_chunks_database_inserter()
        )
