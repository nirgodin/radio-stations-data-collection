from typing import List, Optional, Dict, Tuple

from genie_common.tools import logger, SyncPoolExecutor
from genie_common.utils import read_jsonl
from openai import OpenAI
from openai.types import Batch

from data_collectors.contract import ICollector


class TrackNamesEmbeddingsRetrievalCollector(ICollector):
    def __init__(self, openai: OpenAI, pool_executor: SyncPoolExecutor = SyncPoolExecutor()):
        self._openai = openai
        self._pool_executor = pool_executor

    async def collect(self, batches_ids: List[str]) -> Dict[str, Optional[List[dict]]]:
        results = self._pool_executor.run(iterable=batches_ids, func=self._collect_single_batch, expected_type=tuple)
        return dict(results)

    def _collect_single_batch(self, batch_id: str) -> Tuple[str, Optional[List[dict]]]:
        logger.info(f"Collecting batch `{batch_id}`")
        batch = self._openai.batches.retrieve(batch_id)

        if batch.status == "completed":
            records = self._retrieve_batch_embeddings(batch)
        else:
            logger.info(f"Batch status is `{batch.status}` and not completed. Skipping")
            records = None

        return batch.id, records

    def _retrieve_batch_embeddings(self, batch: Batch) -> List[dict]:
        batch_file = self._openai.files.content(batch.output_file_id)
        data = batch_file.content.decode(encoding="utf-8")

        return read_jsonl(data)
