from abc import ABC, abstractmethod
from typing import List, Type, Iterable, Any

from genie_datastores.postgres.models.orm.base_orm_model import BaseORMModel
from genie_datastores.postgres.operations import insert_records_ignoring_conflicts
from genie_datastores.postgres.utils import query_existing_column_values

from data_collectors.consts.spotify_consts import ID
from data_collectors.contract.inserters.base_postgres_database_inserter import BasePostgresDatabaseInserter
from genie_common.tools import logger


class BaseIDsDatabaseInserter(BasePostgresDatabaseInserter, ABC):
    async def insert(self, iterable: Iterable[Any]) -> List[BaseORMModel]:
        logger.info(f"Starting to run {self.__class__.__name__}")
        raw_records = await self._get_raw_records(iterable)
        records = [getattr(self._orm, self._serialization_method)(record) for record in raw_records]
        valid_records = [record for record in records if isinstance(record, BaseORMModel)]
        unique_records = self._filter_duplicate_ids(valid_records)
        existing_ids = await self._query_existing_ids(unique_records)
        await self._insert_non_existing_records(unique_records, existing_ids)

        return unique_records

    @abstractmethod
    async def _get_raw_records(self, iterable: Iterable[Any]) -> Iterable[Any]:
        raise NotImplementedError

    @property
    @abstractmethod
    def _serialization_method(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def _orm(self) -> Type[BaseORMModel]:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @staticmethod
    def _filter_duplicate_ids(records: List[BaseORMModel]):
        seen_ids = set()
        filtered_records = []

        for record in records:
            if record.id not in seen_ids:
                seen_ids.add(record.id)
                filtered_records.append(record)

        return filtered_records

    async def _query_existing_ids(self, records: List[BaseORMModel]) -> List[str]:
        logger.info(f"Querying database to find existing ids for table `{self._orm.__tablename__}`")
        existing_ids = await query_existing_column_values(
            orm=self._orm,
            column_name=ID,
            records=records,
            engine=self._db_engine
        )
        logger.info(f"Found {len(existing_ids)} existing record in table `{self._orm.__tablename__}`")

        return existing_ids

    async def _insert_non_existing_records(self, records: List[BaseORMModel], existing_ids: List[str]) -> None:
        non_existing_records = []

        for record in records:
            if record.id not in existing_ids:
                non_existing_records.append(record)

        if non_existing_records:
            logger.info(f"Inserting {len(non_existing_records)} records to table {self._orm.__tablename__}")
            await insert_records_ignoring_conflicts(engine=self._db_engine, records=non_existing_records)

        self._log_summary(records, non_existing_records)

    def _log_summary(self, records: List[BaseORMModel], non_existing_records: List[BaseORMModel]) -> None:
        n_non_exist = len(non_existing_records)
        n_exist = len(records) - n_non_exist
        table_name = self._orm.__tablename__

        logger.info(f"Found {n_exist} existing records and {n_non_exist} non existing records in table `{table_name}`")
