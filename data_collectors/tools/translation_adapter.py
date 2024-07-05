from typing import Optional, Any, Dict, Type

from genie_common.clients.google import GoogleTranslateClient
from genie_common.tools import logger
from genie_datastores.postgres.models import Translation
from genie_datastores.models import DataSource, EntityType
from genie_datastores.postgres.operations import execute_query, insert_records
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql import Select


class TranslationAdapter:
    def __init__(self, db_engine: AsyncEngine, translation_client: GoogleTranslateClient):
        self._db_engine = db_engine
        self._translation_client = translation_client

    async def translate(self,
                        text: str,
                        target_language: str,
                        source_language: Optional[str] = None,
                        entity_id: Optional[str] = None,
                        entity_source: Optional[DataSource] = None,
                        entity_type: Optional[EntityType] = None) -> Optional[str]:
        filters = {
            Translation.source_language: source_language,
            Translation.entity_id: entity_id,
            Translation.entity_source: entity_source,
            Translation.entity_type: entity_type
        }
        translation = await self._retrieve_translation_from_cache(
            text=text,
            target_language=target_language,
            filters=filters
        )

        if translation is None:
            translation = await self._fetch_translation(
                text=text,
                target_language=target_language,
                source_language=source_language
            )
            await self._insert_new_translation_record(
                text=text,
                target_language=target_language,
                translation=translation,
                filters=filters
            )

        return translation

    async def _retrieve_translation_from_cache(self,
                                               text: str,
                                               target_language: str,
                                               filters: Dict[Type[Translation], Any]) -> Optional[str]:
        query = self._build_cache_query(text, target_language, filters)
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().first()

    @staticmethod
    def _build_cache_query(text: str, target_language: str, filters: Dict[Type[Translation], Any]) -> Select:
        query = (
            select(Translation.translation)
            .where(Translation.text.ilike(text))
            .where(Translation.target_language == target_language)
        )

        for column, value in filters.items():
            if value is not None:
                query = query.where(column == value)

        return query.limit(1)

    async def _fetch_translation(self, text: str, target_language: str, source_language: str) -> Optional[str]:
        translation_response = await self._translation_client.translate(
            texts=[text],
            target_language=target_language,
            source_language=source_language
        )

        if translation_response:
            first_translation = translation_response[0]
            return first_translation.translation

    async def _insert_new_translation_record(self,
                                             text: str,
                                             target_language: str,
                                             translation: Optional[str],
                                             filters: Dict[Type[Translation], Any]) -> None:
        if translation is None:
            logger.warning(f"Translation request for `{text}` failed. Ignoring")
            return

        kwargs = {column.key: value for column, value in filters.items()}
        additional_kwargs = {
            Translation.text.key: text,
            Translation.target_language.key: target_language,
            Translation.translation.key: translation
        }
        kwargs.update(additional_kwargs)
        record = Translation(**kwargs)

        await insert_records(engine=self._db_engine, records=[record])
