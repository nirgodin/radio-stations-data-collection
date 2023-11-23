from typing import List, Dict, Optional

from postgres_client import TrackIDMapping, SpotifyTrack, SpotifyArtist, execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.logic.updaters import ShazamIDsDatabaseUpdater
from data_collectors.contract import IManager
from data_collectors.logic.collectors.shazam import ShazamSearchCollector
from data_collectors.logic.inserters import ShazamInsertionsManager
from data_collectors.logic.models import MissingTrack
from data_collectors.logs import logger

SHAZAM_MISSING_TRACKS_SELECT_COLUMNS = [
    TrackIDMapping.id,
    TrackIDMapping.shazam_id,
    SpotifyTrack.id,
    SpotifyTrack.artist_id,
    SpotifyTrack.name,
    SpotifyArtist.id,
    SpotifyArtist.name.label("artist_name")
]


class ShazamMissingTracksManager(IManager):
    def __init__(self,
                 db_engine: AsyncEngine,
                 search_collector: ShazamSearchCollector,
                 insertions_manager: ShazamInsertionsManager,
                 ids_updater: ShazamIDsDatabaseUpdater,
                 limit: int):
        self._db_engine = db_engine
        self._search_collector = search_collector
        self._insertions_manager = insertions_manager
        self._ids_updater = ids_updater
        self._limit = limit

    async def run(self):
        logger.info("Started searching Shazam records for missing tracks")
        missing_tracks = await self._query_candidates()
        queries = [track.query for track in missing_tracks]
        query_results = await self._search_collector.collect(queries)
        matched_ids = self._match_spotify_to_shazam_ids(missing_tracks, query_results)

        await self._insert_records(matched_ids)

    async def _query_candidates(self) -> List[MissingTrack]:
        logger.info(f"Started searching spotify tracks with missing shazam ids in tracks_ids_mapping table")
        query = (
            select(*SHAZAM_MISSING_TRACKS_SELECT_COLUMNS)
            .where(TrackIDMapping.id == SpotifyTrack.id)
            .where(SpotifyTrack.artist_id == SpotifyArtist.id)
            .where(TrackIDMapping.shazam_id.is_(None))
            .order_by(TrackIDMapping.update_date.desc())
            .limit(self._limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)
        missing_tracks = [MissingTrack.from_row(row) for row in query_result.all()]
        logger.info(f"Found {len(missing_tracks)} spotify tracks with missing shazam ids in tracks_ids_mapping table")

        return missing_tracks

    def _match_spotify_to_shazam_ids(self,
                                     missing_tracks: List[MissingTrack],
                                     query_results: Dict[str, str]) -> Dict[str, str]:
        logger.info("Matching spotify tracks ids with shazam collector search results")
        matched_ids = {}

        for track in missing_tracks:
            shazam_id = self._find_query_equivalent_id(track, query_results)
            matched_ids[track.spotify_id] = shazam_id

        return matched_ids

    @staticmethod
    def _find_query_equivalent_id(track: MissingTrack, query_results: Dict[str, str]) -> Optional[str]:
        for query, shazam_id in query_results.items():
            if track.query == query:
                query_results.pop(query)

                return shazam_id

        logger.warn(f"Did not find equivalent search record for track `{track.spotify_id}`. Ignoring.")

    async def _insert_records(self, matched_ids: Dict[str, Optional[str]]) -> None:
        non_missing_shazam_ids = [shazam_id for shazam_id in matched_ids.values() if shazam_id is not None]
        await self._insertions_manager.insert(non_missing_shazam_ids)
        await self._ids_updater.update(matched_ids)
