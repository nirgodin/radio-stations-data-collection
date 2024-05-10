import json
import os.path
from tempfile import TemporaryDirectory
from typing import List

from genie_common.models.openai import EmbeddingsModel
from genie_common.tools import logger, SyncPoolExecutor
from openai import OpenAI
from openai.types import FileObject, Batch

from data_collectors.contract import ICollector
from data_collectors.logic.models import MissingTrack


class TrackNamesEmbeddingsCollector(ICollector):
    def __init__(self, openai: OpenAI, pool_executor: SyncPoolExecutor = SyncPoolExecutor()):
        self._openai = openai
        self._pool_executor = pool_executor

    async def collect(self, missing_tracks: List[MissingTrack]) -> str:
        logger.info(f"Creating embeddings batch requests for {len(missing_tracks)} tracks")
        requests = self._pool_executor.run(
            iterable=missing_tracks,
            func=self._create_single_embeddings_request,
            expected_type=dict
        )
        logger.info("Generating embeddings batch input file")
        batch_input_file = self._generate_batch_input_file(requests)
        logger.info(f"Sending batch request with id `{batch_input_file.id}` to OpenAI batch endpoint")
        batch: Batch = self._openai.batches.create(
            input_file_id=batch_input_file.id,
            endpoint="/v1/embeddings",
            completion_window="24h",
        )

        return batch.id

    @staticmethod
    def _create_single_embeddings_request(missing_track: MissingTrack) -> dict:
        body = {
            "input": missing_track.track_name,
            "model": EmbeddingsModel.ADA.value
        }
        return {
            "custom_id": missing_track.spotify_id,
            "method": "POST",
            "url": "/v1/embeddings",
            "body": body
        }

    def _generate_batch_input_file(self, requests: List[dict]) -> FileObject:
        with TemporaryDirectory() as dir_path:
            file_path = os.path.join(dir_path, "embeddings_requests.jsonl")
            self._to_jsonl(requests, file_path)

            with open(file_path, "rb") as file:
                batch_input_file = self._openai.files.create(
                    file=file,
                    purpose="batch"
                )

        return batch_input_file

    @staticmethod
    def _to_jsonl(data: List[dict], file_path: str) -> None:
        with open(file_path, "w") as file:
            for item in data:
                json_str = json.dumps(item)
                file.write(json_str + "\n")
