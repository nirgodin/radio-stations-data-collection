from abc import ABC, abstractmethod
from typing import Type

from postgres_client import BaseORMModel, execute_query
from sqlalchemy import update, case
from sqlalchemy.ext.asyncio import AsyncEngine


class BaseDatabaseUpdater(ABC):
    def __init__(self, db_engine: AsyncEngine):
        self._db_engine = db_engine

    @abstractmethod
    async def update(self, *args, **kwargs) -> None:
        raise NotImplementedError

    async def _update_by_mapping(self,
                                 mapping: dict,
                                 orm: Type[BaseORMModel],
                                 key_column: BaseORMModel,
                                 value_column: BaseORMModel) -> None:
        query = (
            update(orm)
            .where(key_column.in_(mapping.keys()))
            .values(
                {
                    value_column: case(
                        mapping, value=key_column
                    )
                }
            )
        )
        await execute_query(engine=self._db_engine, query=query)
