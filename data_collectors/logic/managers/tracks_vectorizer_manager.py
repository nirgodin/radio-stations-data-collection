import os.path
import pickle
from functools import partial
from tempfile import TemporaryDirectory
from typing import List

from genie_common.tools import logger, SyncPoolExecutor
from genie_datastores.google.drive import GoogleDriveClient, GoogleDriveUploadMetadata
from pandas import DataFrame
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import MinMaxScaler

from data_collectors.consts.milvus_consts import TRACKS_FEATURES_COLLECTION, FEATURES_FIELD_NAME
from data_collectors.consts.musixmatch_consts import TRACK_ID
from data_collectors.consts.spotify_consts import ID
from data_collectors.contract import IManager
from data_collectors.logic.collectors import TracksVectorizerTrainDataCollector
from data_collectors.logic.inserters.milvus import MilvusChunksDatabaseInserter


class TracksVectorizerManager(IManager):
    def __init__(self,
                 train_data_collector: TracksVectorizerTrainDataCollector,
                 milvus_inserter: MilvusChunksDatabaseInserter,
                 google_drive_client: GoogleDriveClient,
                 drive_folder_id: str,
                 pool_executor: SyncPoolExecutor = SyncPoolExecutor()):
        self._train_data_collector = train_data_collector
        self._milvus_inserter = milvus_inserter
        self._google_drive_client = google_drive_client
        self._drive_folder_id = drive_folder_id
        self._pool_executor = pool_executor

    async def run(self) -> None:
        data = await self._train_data_collector.collect()
        training_data = self._pre_process_data(data)
        column_transformer = self._create_column_transformer(training_data)
        records = self._to_records(
            data=data,
            training_data=training_data,
            column_transformer=column_transformer
        )
        await self._milvus_inserter.insert(
            collection_name=TRACKS_FEATURES_COLLECTION,
            records=records
        )
        self._upload_column_transformer(column_transformer)

    @staticmethod
    def _create_column_transformer(training_data: DataFrame) -> ColumnTransformer:
        logger.info("Fitting column transformer")
        pipeline = make_pipeline(
            SimpleImputer(strategy='median'),
            MinMaxScaler()
        )
        column_transformer = ColumnTransformer(
            verbose_feature_names_out=False,
            remainder='passthrough',
            transformers=[
                (
                    'pipeline',
                    pipeline,
                    training_data.columns.tolist()
                )
            ]
        )
        column_transformer.set_output(transform='pandas')
        column_transformer.fit(training_data)

        return column_transformer

    @staticmethod
    def _pre_process_data(data: DataFrame) -> DataFrame:
        logger.info("Pre processing data")
        numeric_data = data.drop(TRACK_ID, axis=1)
        sorted_columns = sorted(numeric_data.columns.tolist())

        return numeric_data[sorted_columns]

    def _to_records(self, data: DataFrame, training_data: DataFrame, column_transformer: ColumnTransformer) -> List[dict]:
        logger.info("Converting data to Milvus records")
        transformed_data = column_transformer.transform(training_data)

        return self._pool_executor.run(
            iterable=data.index.tolist(),
            func=partial(self._create_single_record, data, transformed_data),
            expected_type=dict
        )

    @staticmethod
    def _create_single_record(data: DataFrame, transformed_data: DataFrame, index: int) -> dict:
        features = transformed_data.loc[index]
        return {
            ID: data.at[index, TRACK_ID],
            FEATURES_FIELD_NAME: features.tolist()
        }

    def _upload_column_transformer(self, column_transformer: ColumnTransformer) -> None:
        logger.info("Uploading column transformer pickle to google drive")

        with TemporaryDirectory() as dir_path:
            local_path = self._save_column_transformer(dir_path, column_transformer)
            file_metadata = GoogleDriveUploadMetadata(
                local_path=local_path,
                drive_folder_id=self._drive_folder_id,
                file_name="column_transformer.pkl"
            )
            self._google_drive_client.upload(file_metadata)

    @staticmethod
    def _save_column_transformer(dir_path: str, column_transformer: ColumnTransformer) -> str:
        file_path = os.path.join(dir_path, "column_transformer.pkl")

        with open(file_path, "wb") as f:
            pickle.dump(column_transformer, f)

        return file_path
