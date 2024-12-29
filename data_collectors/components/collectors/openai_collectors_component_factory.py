from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.collectors import (
    TrackNamesEmbeddingsCollector,
    TrackNamesEmbeddingsRetrievalCollector,
)


class OpenAICollectorsComponentFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self._tools = tools

    def get_track_names_embeddings_collector(self) -> TrackNamesEmbeddingsCollector:
        return TrackNamesEmbeddingsCollector(openai=self._tools.get_openai())

    def get_track_names_embeddings_retrieval_collector(
        self,
    ) -> TrackNamesEmbeddingsRetrievalCollector:
        return TrackNamesEmbeddingsRetrievalCollector(openai=self._tools.get_openai())
