from genie_datastores.postgres.operations import get_database_engine

from data_collectors.logic import PostgresDuplicatesDeleter


class DeletersComponentFactory:
    @staticmethod
    def get_postgres_duplicates_deleter() -> PostgresDuplicatesDeleter:
        return PostgresDuplicatesDeleter(get_database_engine())
