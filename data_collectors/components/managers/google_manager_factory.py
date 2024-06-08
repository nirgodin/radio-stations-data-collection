from aiohttp import ClientSession
from genie_datastores.postgres.operations import get_database_engine

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import GoogleArtistsOriginGeocodingManager, GeminiArtistsAboutManager


class GoogleManagerFactory(BaseManagerFactory):
    def get_artists_origin_geocoding_manager(self, session: ClientSession) -> GoogleArtistsOriginGeocodingManager:
        geocoding_collector = self.collectors.google.get_geocoding_collector(session)

        return GoogleArtistsOriginGeocodingManager(
            geocoding_collector=geocoding_collector,
            db_updater=self.updaters.get_values_updater()
        )

    def get_gemini_artists_about_manager(self) -> GeminiArtistsAboutManager:
        return GeminiArtistsAboutManager(
            db_engine=get_database_engine(),
            artists_about_extractor=self.collectors.google.get_artists_about_extractor(),
            pool_executor=self.tools.get_pool_executor(),
            db_updater=self.updaters.get_values_updater()
        )
