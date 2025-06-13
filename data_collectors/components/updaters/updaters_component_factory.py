from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.updaters import (
    TrackIDsMappingDatabaseUpdater,
    ValuesDatabaseUpdater,
)


class UpdatersComponentFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self.tools = tools

    def get_track_ids_updater(self) -> TrackIDsMappingDatabaseUpdater:
        return TrackIDsMappingDatabaseUpdater(self.tools.get_database_engine())

    def get_values_updater(self) -> ValuesDatabaseUpdater:
        return ValuesDatabaseUpdater(
            db_engine=self.tools.get_database_engine(),
            pool_executor=self.tools.get_pool_executor(),
        )
