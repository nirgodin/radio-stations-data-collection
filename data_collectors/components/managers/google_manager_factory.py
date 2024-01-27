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

        return GoogleArtistsOriginGeocodingManager(
            geocoding_collector=geocoding_collector,
            db_updater=self.updaters.get_values_updater(pool_executor)
        )
