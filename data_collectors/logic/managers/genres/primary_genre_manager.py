from typing import Optional, List, Dict

from genie_common.tools import logger
from genie_datastores.postgres.models import (
    Track,
    SpotifyArtist,
    ShazamTrack,
    SpotifyTrack,
    TrackIDMapping,
    PrimaryGenre,
)
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.logic.models import DBUpdateRequest
from data_collectors.logic.updaters import ValuesDatabaseUpdater
from data_collectors.contract import IManager
from data_collectors.logic.analyzers import PrimaryGenreAnalyzer


class PrimaryGenreManager(IManager):
    def __init__(
        self,
        db_engine: AsyncEngine,
        genre_analyzer: PrimaryGenreAnalyzer,
        db_updater: ValuesDatabaseUpdater,
    ):
        self._db_engine = db_engine
        self._genre_analyzer = genre_analyzer
        self._db_updater = db_updater

    async def run(self, limit: Optional[int]):
        tracks_raw_genres = await self._retrieve_tracks_genres(limit)
        tracks_primary_genres = self._genre_analyzer.analyze(tracks_raw_genres)
        update_requests = self._to_update_requests(tracks_primary_genres)
        await self._db_updater.update(update_requests)

    async def _retrieve_tracks_genres(self, limit: Optional[int]) -> List[Row]:
        logger.info("Retrieving tracks without primary genre")
        query = (
            select(
                Track.id,
                SpotifyArtist.genres.label("spotify_genres"),
                ShazamTrack.primary_genre.label("shazam_genre"),
            )
            .where(Track.id == SpotifyTrack.id)
            .where(SpotifyTrack.artist_id == SpotifyArtist.id)
            .where(Track.id == TrackIDMapping.id)
            .where(TrackIDMapping.shazam_id == ShazamTrack.id)
            .where(Track.primary_genre.is_(None))
            .order_by(Track.update_date.asc())
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.all()

    @staticmethod
    def _to_update_requests(tracks_primary_genres: Dict[str, PrimaryGenre]) -> List[DBUpdateRequest]:
        update_requests = []

        for track_id, primary_genre in tracks_primary_genres.items():
            request = DBUpdateRequest(id=track_id, values={Track.primary_genre: primary_genre})
            update_requests.append(request)

        return update_requests
