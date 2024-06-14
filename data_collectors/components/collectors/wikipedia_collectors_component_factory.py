from genie_datastores.postgres.operations import get_database_engine

from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.collectors import WikipediaArtistsExistingDetailsCollector
from data_collectors.logic.collectors.wikipedia import WikipediaAgeNameCollector, WikipediaAgeLinkCollector


class WikipediaCollectorsComponentFactory:
    def __init__(self, tools: ToolsComponentFactory = ToolsComponentFactory()):
        self._tools = tools

    def get_wikipedia_age_name_collector(self) -> WikipediaAgeNameCollector:
        return WikipediaAgeNameCollector(
            pool_executor=self._tools.get_pool_executor(),
            db_engine=get_database_engine()
        )

    def get_wikipedia_age_link_collector(self) -> WikipediaAgeLinkCollector:
        return WikipediaAgeLinkCollector(
            pool_executor=self._tools.get_pool_executor(),
            db_engine=get_database_engine()
        )

    def get_wikipedia_existing_details_collector(self) -> WikipediaArtistsExistingDetailsCollector:
        return WikipediaArtistsExistingDetailsCollector(
            db_engine=get_database_engine(),
            pool_executor=self._tools.get_pool_executor()
        )
