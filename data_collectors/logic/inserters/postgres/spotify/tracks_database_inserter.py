from typing import Type, List

from genie_common.utils import safe_nested_get
from genie_datastores.postgres.models import Track

from data_collectors.consts.spotify_consts import TRACK, ID
from data_collectors.logic.inserters.postgres.spotify.base_spotify_database_inserter import (
    BaseSpotifyDatabaseInserter,
)


class TracksDatabaseInserter(BaseSpotifyDatabaseInserter):
    async def _get_raw_records(self, tracks: List[dict]) -> List[str]:
        ids = {safe_nested_get(track, [TRACK, ID]) for track in tracks}
        return [id_ for id_ in ids if isinstance(id_, str)]

    @property
    def _serialization_method(self) -> str:
        return "from_id"

    @property
    def _orm(self) -> Type[Track]:
        return Track

    @property
    def name(self) -> str:
        return "tracks_computed_fields"
