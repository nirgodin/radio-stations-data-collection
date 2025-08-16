from typing import Optional, Dict, List

from genie_common.tools import logger
from genie_datastores.postgres.models import Artist, SpotifyArtist
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import IManager
from data_collectors.logic.collectors import GeniusArtistsIDsCollector
from data_collectors.logic.models import DBUpdateRequest
from data_collectors.logic.updaters import ValuesDatabaseUpdater


class GeniusArtistsIDsManager(IManager):
    def __init__(
        self,
        db_engine: AsyncEngine,
        artists_ids_collector: GeniusArtistsIDsCollector,
        db_updater: ValuesDatabaseUpdater,
    ):
        self._db_engine = db_engine
        self._artists_ids_collector = artists_ids_collector
        self._db_updater = db_updater

    async def run(self, limit: Optional[int]) -> None:
        logger.info(f"Starting to search for {limit} artists ids")
        artist_id_to_name = await self._query_artists_with_missing_genius_id(limit)

        if not artist_id_to_name:
            logger.info("Did not find any artist with missing genius ids. Aborting")
            return

        spotify_to_genius_id = await self._artists_ids_collector.collect(artist_id_to_name)
        await self._update_artists_genius_ids(spotify_to_genius_id)

    async def _query_artists_with_missing_genius_id(self, limit: Optional[int]) -> Dict[str, str]:
        logger.info("Querying db for artists with missing genius ids")
        query = (
            select(SpotifyArtist.id, SpotifyArtist.name)
            .where(SpotifyArtist.id == Artist.id)
            .where(Artist.genius_id.is_(None))
            .order_by(Artist.update_date.asc())
            .limit(limit)
        )
        cursor = await execute_query(engine=self._db_engine, query=query)
        query_result = cursor.all()

        return {row.id: row.name for row in query_result}

    def _map_spotify_and_genius_artists_ids(
        self, tracks: List[dict], genius_id_artist_id_mapping: Dict[str, str]
    ) -> Dict[str, str]:
        logger.info("Mapping found tracks artists ids to spotify ids")
        spotify_genius_artists_ids_map = {}

        for track in tracks:
            ids_map = self._map_single_track_ids(track, genius_id_artist_id_mapping)

            if ids_map is not None:
                spotify_genius_artists_ids_map.update(ids_map)

        return spotify_genius_artists_ids_map

    async def _update_artists_genius_ids(self, artists_ids_map: Dict[str, str]) -> None:
        logger.info("Updating artists genius ids")
        update_requests = []

        for spotify_id, genius_id in artists_ids_map.items():
            request = DBUpdateRequest(id=spotify_id, values={Artist.genius_id: genius_id})
            update_requests.append(request)

        await self._db_updater.update(update_requests)
