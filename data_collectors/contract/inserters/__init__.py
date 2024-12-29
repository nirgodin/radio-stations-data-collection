from data_collectors.contract.inserters.postgres_database_inserter_interface import (
    IPostgresDatabaseInserter,
)
from data_collectors.contract.inserters.database_inserter_interface import (
    IDatabaseInserter,
)

__all__ = ["IDatabaseInserter", "IPostgresDatabaseInserter"]
