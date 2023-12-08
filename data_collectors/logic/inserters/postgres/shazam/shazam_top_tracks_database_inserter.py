from typing import List, Dict
from datetime import date
from genie_datastores.postgres.models import ShazamTopTrack, ShazamLocation
from genie_datastores.postgres.operations import insert_records

from data_collectors.consts.shazam_consts import KEY
from data_collectors.contract.base_database_inserter import BaseDatabaseInserter
from genie_common.tools import logger


class ShazamTopTracksDatabaseInserter(BaseDatabaseInserter):
    async def insert(self, locations_tracks: Dict[ShazamLocation, List[dict]]) -> List[ShazamTopTrack]:
        logger.info("Starting to insert shazam top tracks to database")
        records = self._to_records(locations_tracks)
        await insert_records(engine=self._db_engine, records=records)  # TODO: Add base class that handles UniqueConstraint existing records
        logger.info("Successfully inserted shazam top tracks to database")

        return records

    def _to_records(self, locations_tracks: Dict[ShazamLocation, List[dict]]) -> List[ShazamTopTrack]:
        records = []
        today = date.today()

        for location, tracks in locations_tracks.items():
            location_records = self._get_location_records(location, tracks, today)
            records.extend(location_records)

        return records

    @staticmethod
    def _get_location_records(location: ShazamLocation, tracks: List[dict], today: date) -> List[ShazamTopTrack]:
        logger.info(f"Turning `{location.value}` tracks to ORM records")
        records = []

        for i, track in enumerate(tracks):
            record = ShazamTopTrack(
                track_id=track[KEY],
                location=location,
                date=today,
                position=i+1
            )
            records.append(record)

        return records
