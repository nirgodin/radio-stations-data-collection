from datetime import date
from typing import List, Dict

from genie_common.tools import logger
from genie_datastores.postgres.models import ShazamTopTrack, ShazamLocation

from data_collectors.consts.spotify_consts import ID
from data_collectors.logic.inserters.postgres import ChunksDatabaseInserter


class ShazamTopTracksDatabaseInserter:
    def __init__(self, chunks_inserter: ChunksDatabaseInserter):
        self._chunks_inserter = chunks_inserter

    async def insert(
        self, locations_tracks: Dict[ShazamLocation, List[dict]]
    ) -> List[ShazamTopTrack]:
        logger.info("Starting to insert shazam top tracks to database")
        records = self._to_records(locations_tracks)
        await self._chunks_inserter.insert(records)
        logger.info("Successfully inserted shazam top tracks to database")

        return records

    def _to_records(
        self, locations_tracks: Dict[ShazamLocation, List[dict]]
    ) -> List[ShazamTopTrack]:
        records = []
        today = date.today()

        for location, tracks in locations_tracks.items():
            location_records = self._get_location_records(location, tracks, today)
            records.extend(location_records)

        return records

    @staticmethod
    def _get_location_records(
        location: ShazamLocation, tracks: List[dict], today: date
    ) -> List[ShazamTopTrack]:
        logger.info(f"Turning `{location.value}` tracks to ORM records")
        records = []

        for i, track in enumerate(tracks):
            record = ShazamTopTrack(
                track_id=track[ID], location=location, date=today, position=i + 1
            )
            records.append(record)

        return records
