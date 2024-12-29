from typing import List, Optional

from genie_common.tools import AioPoolExecutor, logger
from genie_common.utils import chain_lists
from genie_datastores.contract import IDatabaseInserter
from genie_datastores.mongo.models import AboutDocument, AboutParagraphDocument
from google.generativeai import embed_content_async

from data_collectors.logic.inserters.mongo.mongo_chunks_database_inserter import (
    MongoChunksDatabaseInserter,
)
from data_collectors.logic.serializers import ArtistsAboutParagraphsSerializer


class AboutParagraphsDatabaseInserter(IDatabaseInserter):
    def __init__(
        self,
        pool_executor: AioPoolExecutor,
        paragraphs_serializer: ArtistsAboutParagraphsSerializer,
        chunks_inserter: MongoChunksDatabaseInserter,
        embeddings_model: str = "models/embedding-001",
        embeddings_task_type: str = "semantic_similarity",
    ):
        self._pool_executor = pool_executor
        self._paragraphs_serializer = paragraphs_serializer
        self._chunks_inserter = chunks_inserter
        self._embeddings_model = embeddings_model
        self._embeddings_task_type = embeddings_task_type

    async def insert(self, about_documents: List[AboutDocument]) -> None:
        logger.info(f"Serializing {len(about_documents)} to paragraph documents")
        paragraphs_documents: List[List[AboutParagraphDocument]] = (
            await self._pool_executor.run(
                iterable=about_documents,
                func=self._to_paragraph_documents,
                expected_type=list,
            )
        )
        flattened_documents = chain_lists(paragraphs_documents)

        await self._chunks_inserter.insert(flattened_documents, AboutParagraphDocument)

    async def _to_paragraph_documents(
        self, about_document: AboutDocument
    ) -> List[AboutParagraphDocument]:
        paragraphs = self._paragraphs_serializer.serialize(about_document.about)
        documents = []

        for i, paragraph in enumerate(paragraphs):
            document = await self._generate_single_paragraph_document(
                about_document=about_document, text=paragraph, number=i + 1
            )

            if document is not None:
                documents.append(document)

        return documents

    async def _generate_single_paragraph_document(
        self, about_document: AboutDocument, text: str, number: int
    ) -> Optional[AboutParagraphDocument]:
        try:
            embedding = await self._generate_paragraph_embedding(text)
            return AboutParagraphDocument(
                about_id=str(about_document.id),
                embedding=embedding,
                number=number,
                text=text,
            )

        except:
            logger.exception(
                f"Failed to create paragraph document for document id `{about_document.id}`, paragraph number "
                f"{number}, with the following text: `{text}`. Returning None by default"
            )
            return None

    @staticmethod
    async def _generate_paragraph_embedding(content: str) -> List[float]:
        response = await embed_content_async(
            model="models/embedding-001",
            content=content,
            task_type="semantic_similarity",
        )
        return response["embedding"]
