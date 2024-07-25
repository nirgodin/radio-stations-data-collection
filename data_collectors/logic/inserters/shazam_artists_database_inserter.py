from typing import List, Optional

from genie_common.tools import logger, AioPoolExecutor
from genie_common.utils import safe_nested_get
from genie_datastores.contract import IDatabaseInserter
from genie_datastores.models import EntityType, DataSource
from genie_datastores.mongo.models import AboutDocument

from data_collectors.consts.shazam_consts import ATTRIBUTES, ARTIST_BIO
from data_collectors.consts.spotify_consts import ID, NAME
from data_collectors.logic.inserters.postgres import ShazamArtistsPostgresDatabaseInserter


class ShazamArtistsDatabaseInserter(IDatabaseInserter):
    def __init__(self, postgres_inserter: ShazamArtistsPostgresDatabaseInserter, pool_executor: AioPoolExecutor):
        self._postgres_inserter = postgres_inserter
        self._pool_executor = pool_executor

    async def insert(self, records: List[dict]) -> None:
        logger.info("Inserting Shazam Postgres records")
        await self._postgres_inserter.insert(records)
        documents = self._serialize_about_documents(records)

        if not documents:
            logger.info("Did not find any about document. Skipping documents insertion")
            return

        await self._insert_about_documents(documents)

    def _serialize_about_documents(self, records: List[dict]) -> List[AboutDocument]:
        logger.info(f"Serializing {len(records)} Shazam artist records to about documents")
        documents = []

        for record in records:
            about = self._to_about_document(record)

            if about is not None:
                documents.append(about)

        return documents

    @staticmethod
    def _to_about_document(record: dict) -> Optional[AboutDocument]:
        about = safe_nested_get(record, [ATTRIBUTES, ARTIST_BIO])

        if about:
            return AboutDocument(
                about=about,
                entity_type=EntityType.ARTIST,
                entity_id=record[ID],
                name=safe_nested_get(record, [ATTRIBUTES, NAME]),
                source=DataSource.SHAZAM
            )

    async def _insert_about_documents(self, documents: List[AboutDocument]) -> None:
        results = await self._pool_executor.run(
            iterable=documents,
            func=self._insert_single_about_document,
            expected_type=bool
        )
        total_documents_number= len(documents)
        valid_results_number = len(results)
        non_existing_documents = [result for result in results if result is False]
        non_existing_documents_number = len(non_existing_documents)
        existing_documents_number = valid_results_number - non_existing_documents_number
        errors_number = total_documents_number - valid_results_number

        logger.info(
            f"Out of {total_documents_number} Shazam about documents, {non_existing_documents_number} were inserted, "
            f"{existing_documents_number} already exist, and {errors_number} encountered an error during insertion"
        )

    @staticmethod
    async def _insert_single_about_document(document: AboutDocument) -> bool:
        existing_document = await AboutDocument.find_one(
            AboutDocument.entity_id == document.entity_id,
            AboutDocument.source == document.source
        )
        if existing_document:
            return True

        await AboutDocument.insert_one(document)
