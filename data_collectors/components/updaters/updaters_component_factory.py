from genie_common.tools import AioPoolExecutor
from genie_datastores.postgres.operations import get_database_engine

from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.updaters import (
    TrackIDsMappingDatabaseUpdater,
    BillboardTracksDatabaseUpdater,
    ValuesDatabaseUpdater
)


class UpdatersComponentFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self.tools = tools

    @staticmethod
    def get_billboard_tracks_updater() -> BillboardTracksDatabaseUpdater:
        return BillboardTracksDatabaseUpdater(get_database_engine())

    @staticmethod
    def get_track_ids_updater() -> TrackIDsMappingDatabaseUpdater:
        return TrackIDsMappingDatabaseUpdater(get_database_engine())

    def get_values_updater(self) -> ValuesDatabaseUpdater:
        return ValuesDatabaseUpdater(
            db_engine=get_database_engine(),
            pool_executor=self.tools.get_pool_executor()
        )
