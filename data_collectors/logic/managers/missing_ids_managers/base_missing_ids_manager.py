from abc import ABC, abstractmethod
from typing import Optional, List, Dict

from genie_datastores.postgres.models import TrackIDMapping, SpotifyTrack, SpotifyArtist
from genie_datastores.postgres.models.orm.base_orm_model import BaseORMModel
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.musixmatch_consts import ARTIST_NAME
from data_collectors.logic.updaters import TrackIDsMappingDatabaseUpdater
from data_collectors.contract import IManager, BaseSearchCollector
from data_collectors.logic.models import MissingTrack
from genie_common.tools import logger

MISSING_TRACKS_SELECT_COLUMNS = [
    TrackIDMapping.id,
    SpotifyTrack.id,
    SpotifyTrack.artist_id,
    SpotifyTrack.name,
    SpotifyArtist.id,
    SpotifyArtist.name.label(ARTIST_NAME)
]


class BaseMissingIDsManager(IManager, ABC):
    def __init__(self,
                 db_engine: AsyncEngine,
                 search_collector: BaseSearchCollector,
                 track_ids_updater: TrackIDsMappingDatabaseUpdater):
        self._db_engine = db_engine
        self._search_collector = search_collector
        self._track_ids_updater = track_ids_updater

    async def run(self, limit: Optional[int]):
        logger.info(f"Started searching tracks with missing `{self._column}` values in tracks_ids_mapping table")
        missing_tracks = await self._query_candidates(limit)
        search_results = await self._search_collector.collect(missing_tracks)
        matched_ids = self._match_spotify_to_column_ids(missing_tracks, search_results)
        await self._track_ids_updater.update(ids_mapping=matched_ids, value_column=self._column)
        await self._insert_additional_records(matched_ids)

    @property
    @abstractmethod
    def _column(self) -> TrackIDMapping:
        raise NotImplementedError

    @abstractmethod
    async def _insert_additional_records(self, matched_ids: Dict[str, str]) -> None:
        raise NotImplementedError

    @property
    def _query_columns(self) -> List[BaseORMModel]:
        return MISSING_TRACKS_SELECT_COLUMNS + [self._column]

    async def _query_candidates(self, limit: Optional[int]) -> List[MissingTrack]:
        logger.info(f"Querying database for missing ids")
        query = (
            select(*self._query_columns)
            .where(TrackIDMapping.id == SpotifyTrack.id)
            .where(SpotifyTrack.artist_id == SpotifyArtist.id)
            .where(self._column.is_(None))
            .order_by(TrackIDMapping.update_date.desc())
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)
        missing_tracks = [MissingTrack.from_row(row) for row in query_result.all()]
        logger.info(f"Found {len(missing_tracks)} spotify tracks with missing ids in tracks_ids_mapping table")

        return missing_tracks

    def _match_spotify_to_column_ids(self,
                                     missing_tracks: List[MissingTrack],
                                     query_results: Dict[MissingTrack, Optional[str]]) -> Dict[str, str]:
        logger.info("Matching spotify tracks ids with search collector results")
        matched_ids = {}

        for track in missing_tracks:
            matched_id = self._match_query_id(track, query_results)
            matched_ids[track.spotify_id] = matched_id

        return matched_ids

    @staticmethod
    def _match_query_id(track: MissingTrack, query_results: Dict[MissingTrack, Optional[str]]) -> Optional[str]:
        for missing_track, matched_id in query_results.items():
            if track == missing_track:
                query_results.pop(missing_track)

                return matched_id

        logger.warn(f"Did not find equivalent search record for track `{track.spotify_id}`. Ignoring.")
