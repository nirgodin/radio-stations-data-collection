from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.collectors import WikipediaArtistsExistingDetailsCollector


class WikipediaCollectorsComponentFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self._tools = tools

    def get_wikipedia_existing_details_collector(
        self,
    ) -> WikipediaArtistsExistingDetailsCollector:
        return WikipediaArtistsExistingDetailsCollector(self._tools.get_database_engine())
