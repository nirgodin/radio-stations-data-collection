from typing import List, Type, Any, Optional

from genie_common.tools import logger
from genie_common.utils import safe_nested_get
from genie_datastores.postgres.models import AudioFeatures
from genie_datastores.postgres.models.orm.spotify.base_spotify_orm_model import (
    BaseSpotifyORMModel,
)
from spotipyio import SpotifyClient
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.audio_features_consts import (
    ACOUSTICNESS,
    DANCEABILITY,
    DURATION_MS,
    ENERGY,
    INSTRUMENTALNESS,
    KEY,
    LIVENESS,
    LOUDNESS,
    MODE,
    SPEECHINESS,
    TEMPO,
    TIME_SIGNATURE,
    VALENCE,
)
from data_collectors.consts.spotify_consts import TRACK, ID
from data_collectors.logic.inserters.postgres import BaseIDsDatabaseInserter


class SpotifyAudioFeaturesDatabaseInserter(BaseIDsDatabaseInserter):
    def __init__(self, db_engine: AsyncEngine, spotify_client: SpotifyClient):
        super().__init__(db_engine)
        self._spotify_client = spotify_client

    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        ids = {safe_nested_get(track, [TRACK, ID]) for track in tracks}
        valid_ids = sorted([id_ for id_ in ids if isinstance(id_, str)])

        return await self._spotify_client.tracks.audio_features.run(valid_ids)

    def _to_record(self, response: Any) -> Optional[AudioFeatures]:
        if not self._is_valid_response(response):
            logger.warning("Received invalid audio features response from Spotify. Returning None")
            return None

        return AudioFeatures(
            id=response[ID],
            acousticness=self._safe_multiply_round(response[ACOUSTICNESS]),
            danceability=self._safe_multiply_round(response[DANCEABILITY]),
            duration_ms=response[DURATION_MS],
            energy=self._safe_multiply_round(response[ENERGY]),
            instrumentalness=self._safe_multiply_round(response[INSTRUMENTALNESS]),
            key=response[KEY],
            liveness=self._safe_multiply_round(response[LIVENESS]),
            loudness=response[LOUDNESS],
            mode=response[MODE],
            speechiness=self._safe_multiply_round(response[SPEECHINESS]),
            tempo=self._safe_round(response[TEMPO]),
            time_signature=response[TIME_SIGNATURE],
            valence=self._safe_multiply_round(response[VALENCE]),
        )

    @staticmethod
    def _is_valid_response(response: Any) -> bool:
        if isinstance(response, dict):
            return ID in response.keys()

        return False

    @staticmethod
    def _safe_multiply_round(value: Optional[float]) -> Optional[int]:
        if value is not None:
            return round(value * 100)

    @staticmethod
    def _safe_round(value: Optional[float]) -> Optional[int]:
        if value is not None:
            return round(value)

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return AudioFeatures

    @property
    def name(self) -> str:
        return "audio_features"
