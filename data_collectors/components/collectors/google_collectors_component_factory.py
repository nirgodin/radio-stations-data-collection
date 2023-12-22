from aiohttp import ClientSession
from genie_common.tools import AioPoolExecutor
from genie_datastores.postgres.operations import get_database_engine

from data_collectors.logic.collectors import GoogleGeocodingCollector


class GoogleCollectorsComponentFactory:
    @staticmethod
    def get_geocoding_collector(session: ClientSession, pool_executor: AioPoolExecutor) -> GoogleGeocodingCollector:
        return GoogleGeocodingCollector(
            db_engine=get_database_engine(),
            pool_executor=pool_executor,
            session=session
        )
