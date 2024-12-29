from abc import ABC, abstractmethod
from typing import List, Type, Tuple

from genie_common.tools import logger
from genie_datastores.postgres.models import BaseORMModel
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import tuple_, select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import IPostgresDatabaseInserter
from data_collectors.logic.inserters.postgres.chunks_database_inserter import (
    ChunksDatabaseInserter,
)


class BaseUniqueDatabaseInserter(IPostgresDatabaseInserter, ABC):
    def __init__(self, db_engine: AsyncEngine, chunks_inserter: ChunksDatabaseInserter):
        super().__init__(db_engine)
        self._chunks_inserter = chunks_inserter

    async def insert(self, records: List[BaseORMModel]) -> None:
        logger.info(
            f"{self.__class__.__name__} received {len(records)} records to insert"
        )
        non_existing_records = await self._filter_out_existing_records(records)

        if non_existing_records:
            await self._chunks_inserter.insert(non_existing_records)
        else:
            logger.info("Did not find any new record to insert. Skipping.")

    async def _filter_out_existing_records(
        self, records: List[BaseORMModel]
    ) -> List[BaseORMModel]:
        logger.info("Starting to identify and remove existing records")
        non_existing_records = []
        existing_records = await self._get_existing_records(records)

        for record in records:
            if self._is_new_record(record, existing_records):
                non_existing_records.append(record)
            else:
                logger.debug(
                    f"Record with the table unique identifiers already exists. Skipping"
                )

        logger.info(
            f"Found {len(non_existing_records)} non existing records out of {len(existing_records)} given"
        )
        return non_existing_records

    async def _get_existing_records(
        self, records: List[BaseORMModel]
    ) -> List[BaseORMModel]:
        filter_columns = tuple_(*self._get_orm_unique_attributes(self._orm))
        records_columns = [
            self._get_orm_unique_attributes(record) for record in records
        ]
        query = select(self._orm).where(filter_columns.in_(records_columns))
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().all()

    def _get_orm_unique_attributes(self, orm: BaseORMModel) -> Tuple[BaseORMModel]:
        attributes: List[BaseORMModel] = [
            getattr(orm, attribute) for attribute in self._unique_attributes
        ]
        return tuple(attributes)

    def _is_new_record(
        self, candidate: BaseORMModel, existing_records: List[Row]
    ) -> bool:
        return not any(
            self._has_same_unique_attributes(candidate, record)
            for record in existing_records
        )

    def _has_same_unique_attributes(self, candidate: BaseORMModel, record: Row) -> bool:
        are_attributes_identical = []

        for attribute in self._unique_attributes:
            candidate_attribute = getattr(candidate, attribute)
            record_attribute = getattr(record, attribute)
            are_identical = candidate_attribute == record_attribute
            are_attributes_identical.append(are_identical)

        return all(are_attributes_identical)

    @property
    @abstractmethod
    def _unique_attributes(self) -> List[str]:
        raise NotImplementedError

    @property
    @abstractmethod
    def _orm(self) -> Type[BaseORMModel]:
        raise NotImplementedError
