from typing import List, Type

from genie_datastores.postgres.models import BaseORMModel
from genie_datastores.postgres.operations import read_sql, execute_query
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql import Select

from data_collectors.contract import IDatabaseDeleter


class PostgresDuplicatesDeleter(IDatabaseDeleter):
    def __init__(self, db_engine: AsyncEngine):
        self._db_engine = db_engine

    async def delete(self, orm: Type[BaseORMModel], subset: List[str], primary_key: str) -> None:
        select_query = self._build_select_query(orm, subset)
        data = await read_sql(engine=self._db_engine, query=select_query)
        duplicates = data[data.duplicated(subset=subset)]
        duplicate_primary_keys = duplicates[primary_key].tolist()
        primary_key_orm = getattr(orm, primary_key)
        delete_query = (
            delete(orm)
            .where(primary_key_orm.in_(duplicate_primary_keys))
        )

        await execute_query(engine=self._db_engine, query=delete_query)

    @staticmethod
    def _build_select_query(orm: Type[BaseORMModel], subset: List[str]) -> Select:
        query = select(orm)

        for column in subset:
            column_orm = getattr(orm, column)
            query = query.where(column_orm.isnot(None))

        return query
