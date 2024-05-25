from abc import abstractmethod
from typing import Optional, Dict, Tuple, Type

from database.orm_models.base_orm_model import BaseORMModel
from genie_common.clients.google import GoogleTranslateClient
from genie_common.tools import AioPoolExecutor, logger
from genie_datastores.postgres.models import Translation, DataSource, EntityType
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql import Select

from data_collectors.contract import IManager
from data_collectors.logic.inserters.postgres import ChunksDatabaseInserter


class BaseTranslationManager(IManager):
    def __init__(self,
                 db_engine: AsyncEngine,
                 pool_executor: AioPoolExecutor,
                 translation_client: GoogleTranslateClient,
                 chunks_inserter: ChunksDatabaseInserter):
        self._db_engine = db_engine
        self._pool_executor = pool_executor
        self._translation_client = translation_client
        self._chunks_inserter = chunks_inserter

    async def run(self, limit: Optional[int]) -> None:
        logger.info(f"Starting to run `{self.__class__.__name__}` for {limit} entries")
        entity_ids_names_map = await self._query_relevant_entities(limit)

        if not entity_ids_names_map:
            logger.info("Did not find any relevant entity to translate. Aborting")
            return

        logger.info(f"Translating {len(entity_ids_names_map)} names")
        records = await self._pool_executor.run(
            iterable=entity_ids_names_map.items(),
            func=self._translate_single_record,
            expected_type=Translation
        )
        await self._chunks_inserter.insert(records)

    async def _query_relevant_entities(self, limit: Optional[int]) -> Dict[str, str]:
        logger.info(f"Querying database for relevant entities")
        translations_subquery = (
            select(Translation.id)
            .where(Translation.entity_source == self._entity_source)
            .where(Translation.entity_type == self._entity_type)
        )
        query = (
            self._query
            .where(self._orm.id.notin_(translations_subquery))
            .limit(limit)
        )
        raw_result = await execute_query(engine=self._db_engine, query=query)
        query_result = raw_result.all()

        return {entity.id: entity.name for entity in query_result}

    async def _translate_single_record(self, entity_id_and_name: Tuple[str, str]) -> Translation:
        entity_id, entity_name = entity_id_and_name
        translation_response = await self._translation_client.translate(
            texts=[entity_name],
            target_language="he",
            source_language="en"
        )

        if translation_response:
            first_translation = translation_response[0]
            return Translation(
                id=entity_id,
                entity_source=self._entity_source,
                entity_type=self._entity_type,
                text=entity_name,
                translation=first_translation.translation,
                source_language=first_translation.source_language,
                target_language=first_translation.target_language
            )

    @property
    @abstractmethod
    def _query(self) -> Select:
        raise NotImplementedError

    @property
    @abstractmethod
    def _orm(self) -> Type[BaseORMModel]:
        raise NotImplementedError

    @property
    @abstractmethod
    def _entity_source(self) -> DataSource:
        raise NotImplementedError

    @property
    @abstractmethod
    def _entity_type(self) -> EntityType:
        raise NotImplementedError
