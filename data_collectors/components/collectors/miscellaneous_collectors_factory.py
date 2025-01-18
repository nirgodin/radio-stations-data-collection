from data_collectors import ShazamTopTracksStatusCollector
from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.collectors import (
    RadioTracksStatusCollector,
    TracksVectorizerTrainDataCollector,
)


class MiscellaneousCollectorsFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self._tools = tools

    def get_tracks_vectorizer_train_data_collector(
        self,
    ) -> TracksVectorizerTrainDataCollector:
        return TracksVectorizerTrainDataCollector(self._tools.get_database_engine())

    def get_radio_tracks_status_collector(self) -> RadioTracksStatusCollector:
        return RadioTracksStatusCollector(self._tools.get_database_engine())

    def get_shazam_top_tracks_status_collector(self) -> ShazamTopTracksStatusCollector:
        return ShazamTopTracksStatusCollector(self._tools.get_database_engine())
