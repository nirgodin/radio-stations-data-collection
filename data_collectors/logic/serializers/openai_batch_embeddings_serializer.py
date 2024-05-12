from typing import List, Dict, Optional

from genie_common.tools import logger
from genie_common.utils import safe_nested_get

from data_collectors.consts.milvus_consts import EMBEDDINGS
from data_collectors.consts.openai_consts import CUSTOM_ID, BODY, DATA, RESPONSE, EMBEDDING
from data_collectors.consts.spotify_consts import ID, NAME
from data_collectors.contract import ISerializer


class OpenAIBatchEmbeddingsSerializer(ISerializer):
    def serialize(self, batch_records: List[dict], track_id_name_mapping: Dict[str, str]) -> List[dict]:
        logger.info("Serializing batch records to embeddings data records")
        records = []

        for batch_record in batch_records:
            record = self._serialize_single_record(batch_record, track_id_name_mapping)

            if record is not None:
                records.append(record)

        return records

    def _serialize_single_record(self, record: dict, track_id_name_mapping: Dict[str, str]) -> Optional[dict]:
        track_id = record[CUSTOM_ID]

        if record.get("error") is None:
            return {
                ID: track_id,
                NAME: track_id_name_mapping[track_id],
                EMBEDDINGS: self._extract_track_embeddings(record)
            }

        logger.warning(f"Track {track_id} was marked as error. Skipping insertion")

    @staticmethod
    def _extract_track_embeddings(record: dict) -> List[float]:
        data = safe_nested_get(record, [RESPONSE, BODY, DATA])

        if data:
            first_record = data[0]
            return first_record[EMBEDDING]
