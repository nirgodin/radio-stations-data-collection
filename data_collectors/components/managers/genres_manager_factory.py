from genie_datastores.postgres.operations import get_database_engine

from data_collectors.logic.managers import PrimaryGenreManager, GenresMappingManager, GenresArtistsOriginManager
from data_collectors.components.managers.base_manager_factory import BaseManagerFactory


class GenresManagerFactory(BaseManagerFactory):
    async def get_primary_genre_manager(self) -> PrimaryGenreManager:
        pool_executor = self.tools.get_pool_executor()
        genre_analyzer = await self.analyzers.get_primary_genre_analyzer()

        return PrimaryGenreManager(
            db_engine=get_database_engine(),
            genre_analyzer=genre_analyzer,
            db_updater=self.updaters.get_values_updater(pool_executor)
        )

    def get_genres_mapping_manager(self) -> GenresMappingManager:
        chunks_generator = self.tools.get_chunks_generator()
        return GenresMappingManager(
            sheets_client=self.tools.get_google_sheets_client(),
            genres_inserter=self.inserters.get_genres_inserter(chunks_generator)
        )

    def get_genres_artists_origin_manager(self) -> GenresArtistsOriginManager:
        pool_executor = self.tools.get_pool_executor()
        chunk_generator = self.tools.get_chunks_generator(pool_executor)

        return GenresArtistsOriginManager(
            db_engine=get_database_engine(),
            db_updater=self.updaters.get_values_updater(pool_executor),
            db_inserter=self.inserters.get_chunks_database_inserter(chunk_generator)
        )
