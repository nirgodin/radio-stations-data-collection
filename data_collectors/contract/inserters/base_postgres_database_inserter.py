from abc import ABC, abstractmethod
from typing import List

from genie_datastores.postgres.models.orm.base_orm_model import BaseORMModel
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract.inserters.database_inserter_interface import IDatabaseInserter


class BasePostgresDatabaseInserter(IDatabaseInserter, ABC):
    def __init__(self, db_engine: AsyncEngine):
        self._db_engine = db_engine
