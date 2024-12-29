from typing import List, Type

from genie_datastores.postgres.models import AudioFeatures
from genie_datastores.postgres.models.orm.spotify.base_spotify_orm_model import (
    BaseSpotifyORMModel,
)

from spotipyio import SpotifyClient
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.spotify_consts import TRACK, ID
from data_collectors.logic.inserters.postgres.spotify.base_spotify_database_inserter import (
    BaseSpotifyDatabaseInserter,
)
from genie_common.utils import safe_nested_get


class SpotifyAudioFeaturesDatabaseInserter(BaseSpotifyDatabaseInserter):
    def __init__(self, db_engine: AsyncEngine, spotify_client: SpotifyClient):
        super().__init__(db_engine)
        self._spotify_client = spotify_client

    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        ids = {safe_nested_get(track, [TRACK, ID]) for track in tracks}
        return await self._spotify_client.tracks.audio_features.run(sorted(ids))

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return AudioFeatures

    @property
    def name(self) -> str:
        return "audio_features"
