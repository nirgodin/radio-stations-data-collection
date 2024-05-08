from genie_datastores.postgres.operations import get_database_engine

from data_collectors.logic.collectors import TracksVectorizerTrainDataCollector


class MiscellaneousCollectorsFactory:
    @staticmethod
    def get_tracks_vectorizer_train_data_collector() -> TracksVectorizerTrainDataCollector:
        return TracksVectorizerTrainDataCollector(get_database_engine())