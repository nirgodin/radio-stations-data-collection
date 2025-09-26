from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.collectors import (
    ChartsCountStatusCollector,
    RadioTracksStatusCollector,
    TracksVectorizerTrainDataCollector,
    RadioTracksTopTracksStatusCollector,
    ShazamTopTracksStatusCollector,
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

    def get_radio_tracks_top_tracks_status_collector(self) -> RadioTracksTopTracksStatusCollector:
        return RadioTracksTopTracksStatusCollector(db_engine=self._tools.get_database_engine())

    def get_charts_count_status_collector(self) -> ChartsCountStatusCollector:
        return ChartsCountStatusCollector(db_engine=self._tools.get_database_engine())
