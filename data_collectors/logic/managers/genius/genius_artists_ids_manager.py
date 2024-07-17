from typing import Optional, Dict, List

from genie_common.tools import logger
from genie_common.utils import safe_nested_get
from genie_datastores.postgres.models import TrackIDMapping, SpotifyTrack, Artist
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.genius_consts import PRIMARY_ARTIST
from data_collectors.consts.spotify_consts import ID
from data_collectors.contract import IManager
from data_collectors.logic.collectors import GeniusTracksCollector
from data_collectors.logic.models import GeniusTextFormat, DBUpdateRequest
from data_collectors.logic.updaters import ValuesDatabaseUpdater


class GeniusArtistsIDsManager(IManager):
    def __init__(self,
                 db_engine: AsyncEngine,
                 tracks_collector: GeniusTracksCollector,
                 db_updater: ValuesDatabaseUpdater):
        self._db_engine = db_engine
        self._tracks_collector = tracks_collector
        self._db_updater = db_updater

    async def run(self, limit: Optional[int]) -> None:
        logger.info(f"Starting to search for {limit} artists ids")
        genius_id_artist_id_mapping = await self._query_genius_id_to_artist_id_map(limit)

        if not genius_id_artist_id_mapping:
            logger.info("Did not find any missing genius ids. Aborting")
            return

        await self._collect_and_update_artists_ids(genius_id_artist_id_mapping)

    async def _query_genius_id_to_artist_id_map(self, limit: Optional[int]) -> Dict[str, str]:
        logger.info("Querying db for genius tracks ids and spotify artists ids")
        query = (
            select(TrackIDMapping.genius_id, Artist.id)
            .where(TrackIDMapping.id == SpotifyTrack.id)
            .where(SpotifyTrack.artist_id == Artist.id)
            .where(TrackIDMapping.genius_id.isnot(None))
            .where(Artist.genius_id.is_(None))
            .limit(limit)
        )
        cursor = await execute_query(engine=self._db_engine, query=query)
        query_result = cursor.all()

        return {row.genius_id: row.id for row in query_result}

    def _map_spotify_and_genius_artists_ids(self,
                                            tracks: List[dict],
                                            genius_id_artist_id_mapping: Dict[str, str]) -> Dict[str, str]:
        logger.info("Mapping found tracks artists ids to spotify ids")
        spotify_genius_artists_ids_map = {}

        for track in tracks:
            ids_map = self._map_single_track_ids(track, genius_id_artist_id_mapping)

            if ids_map is not None:
                spotify_genius_artists_ids_map.update(ids_map)

        return spotify_genius_artists_ids_map

    async def _collect_and_update_artists_ids(self, genius_id_artist_id_mapping: Dict[str, str]) -> None:
        track_ids = list(genius_id_artist_id_mapping.keys())
        tracks = await self._tracks_collector.collect(track_ids, GeniusTextFormat.PLAIN)

        if not tracks:
            logger.warning("Did not receive any valid response from tracks collector. Aborting")
            return

        artists_ids_map = self._map_spotify_and_genius_artists_ids(tracks, genius_id_artist_id_mapping)
        await self._update_artists_genius_ids(artists_ids_map)

    def _map_single_track_ids(self, track: dict, spotify_genius_artists_ids_map: Dict[str, str]) -> Optional[Dict[str, str]]:
        track_id = self._extract_genius_id(track, paths=[ID])

        if track_id is not None:
            spotify_artist_id = spotify_genius_artists_ids_map[track_id]
            genius_artist_id = self._extract_genius_id(track, [PRIMARY_ARTIST, ID])

            if genius_artist_id is not None:
                return {spotify_artist_id: genius_artist_id}

    @staticmethod
    def _extract_genius_id(track: dict, paths: List[str]) -> Optional[str]:
        id_ = safe_nested_get(track, paths)
        return None if id_ is None else str(id_)

    async def _update_artists_genius_ids(self, artists_ids_map: Dict[str, str]) -> None:
        logger.info("Updating artists genius ids")
        update_requests = []

        for spotify_id, genius_id in artists_ids_map.items():
            request = DBUpdateRequest(
                id=spotify_id,
                values={Artist.genius_id: genius_id}
            )
            update_requests.append(request)

        await self._db_updater.update(update_requests)
