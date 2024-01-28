from typing import Type, List

from genie_common.utils import safe_nested_get
from genie_datastores.postgres.models import Track

from data_collectors.consts.spotify_consts import TRACK, ID
from data_collectors.logic.inserters.postgres import BaseSpotifyDatabaseInserter


class TracksDatabaseInserter(BaseSpotifyDatabaseInserter):
    async def _get_raw_records(self, tracks: List[dict]) -> List[str]:
        ids = {safe_nested_get(track, [TRACK, ID]) for track in tracks}
        return list(ids)

    @property
    def _serialization_method(self) -> str:
        return "from_id"

    @property
    def _orm(self) -> Type[Track]:
        return Track

    @property
    def name(self) -> str:
        return "tracks_computed_fields"
