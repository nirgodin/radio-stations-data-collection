from typing import Type, List

from genie_common.utils import safe_nested_get
from genie_datastores.postgres.models import Track

from data_collectors.consts.spotify_consts import TRACK, ID
from data_collectors.logic.inserters.postgres import BaseIDsDatabaseInserter


class TracksDatabaseInserter(BaseIDsDatabaseInserter):
    async def _get_raw_records(self, tracks: List[dict]) -> List[str]:
        ids = {safe_nested_get(track, [TRACK, ID]) for track in tracks}
        return [id_ for id_ in ids if isinstance(id_, str)]

    def _to_record(self, raw_record: str) -> Track:
        return Track(id=raw_record)

    @property
    def _orm(self) -> Type[Track]:
        return Track

    @property
    def name(self) -> str:
        return "tracks_computed_fields"
