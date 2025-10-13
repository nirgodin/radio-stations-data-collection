from typing import Type, List, Any, Optional

from genie_common.utils import safe_nested_get
from genie_datastores.postgres.models import TrackIDMapping
from genie_datastores.postgres.models.orm.spotify.base_spotify_orm_model import (
    BaseSpotifyORMModel,
)

from data_collectors.consts.spotify_consts import TRACK, ID
from data_collectors.logic.inserters.postgres import BaseIDsDatabaseInserter


class TrackIDMappingDatabaseInserter(BaseIDsDatabaseInserter):
    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        return tracks

    def _to_record(self, raw_record: dict) -> Optional[TrackIDMapping]:
        if self._is_serializable(raw_record):
            return TrackIDMapping(id=safe_nested_get(raw_record, [TRACK, ID]))

    @staticmethod
    def _is_serializable(raw: Any) -> bool:
        if isinstance(raw, dict):
            inner_track = raw.get(TRACK)

            if isinstance(inner_track, dict):
                return isinstance(inner_track.get(ID), str)

        return False

    @property
    def name(self) -> str:
        return "track_ids_mapping"

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return TrackIDMapping
