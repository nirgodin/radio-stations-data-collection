from aiohttp import ClientSession

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import GoogleArtistsOriginGeocodingManager


class GoogleManagerFactory(BaseManagerFactory):
    def get_artists_origin_geocoding_manager(self, session: ClientSession) -> GoogleArtistsOriginGeocodingManager:
        pool_executor = self.tools.get_pool_executor()
        geocoding_collector = self.collectors.google.get_geocoding_collector(
            session=session,
            pool_executor=pool_executor
        )
        artists_updater = self.updaters.get_artists_updater(pool_executor)

        return GoogleArtistsOriginGeocodingManager(
            geocoding_collector=geocoding_collector,
            artists_updater=artists_updater
        )
