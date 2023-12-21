from genie_common.tools import AioPoolExecutor
from genie_datastores.postgres.operations import get_database_engine

from data_collectors.logic.collectors.wikipedia import WikipediaAgeNameCollector, WikipediaAgeLinkCollector


class WikipediaCollectorsComponentFactory:
    @staticmethod
    def get_wikipedia_age_name_collector(pool_executor: AioPoolExecutor) -> WikipediaAgeNameCollector:
        return WikipediaAgeNameCollector(
            pool_executor=pool_executor,
            db_engine=get_database_engine()
        )

    @staticmethod
    def get_wikipedia_age_link_collector(pool_executor: AioPoolExecutor) -> WikipediaAgeLinkCollector:
        return WikipediaAgeLinkCollector(
            pool_executor=pool_executor,
            db_engine=get_database_engine()
        )
