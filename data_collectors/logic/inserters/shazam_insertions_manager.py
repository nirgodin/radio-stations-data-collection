from typing import Dict, List, Optional

from postgres_client import BaseORMModel

from data_collectors.consts.shazam_consts import ADAM_ID
from data_collectors.consts.spotify_consts import ARTISTS
from data_collectors.logic.collectors import ShazamArtistsCollector, ShazamTracksCollector
from data_collectors.logic.inserters.shazam.shazam_artists_database_inserter import ShazamArtistsDatabaseInserter
from data_collectors.logic.inserters.shazam.shazam_tracks_database_inserter import ShazamTracksDatabaseInserter
from data_collectors.logs import logger


class ShazamInsertionsManager:
    def __init__(self,
                 artists_collector: ShazamArtistsCollector,
                 tracks_collector: ShazamTracksCollector,
                 artists_inserter: ShazamArtistsDatabaseInserter,
                 tracks_inserter: ShazamTracksDatabaseInserter):
        self._artists_collector = artists_collector
        self._tracks_collector = tracks_collector
        self._ordered_inserters = [
            artists_inserter,
            tracks_inserter
        ]

    async def insert(self, ids: List[str]) -> Dict[str, BaseORMModel]:
        ordered_records = await self._fetch_raw_records(ids)
        serialized_records = {}

        for inserter, records in zip(self._ordered_inserters, ordered_records):
            inserted_records = await inserter.insert(records)
            serialized_records[inserter.name] = inserted_records

        return serialized_records

    async def _fetch_raw_records(self, ids: List[str]) -> List[List[dict]]:
        logger.info(f"Starting to collect tracks and artists from shazam for {len(ids)} tracks ids")
        tracks = await self._tracks_collector.collect(ids)
        artists_ids = {self._extract_artist_id(track) for track in tracks}
        artists = await self._artists_collector.collect(list(artists_ids))
        logger.info("Successfully retrieved tracks and artists from shazam")

        return [artists, tracks]

    @staticmethod
    def _extract_artist_id(track: dict) -> Optional[str]:
        artists = track.get(ARTISTS, [])

        if artists:
            first_artist = artists[0]
            return first_artist.get(ADAM_ID)
