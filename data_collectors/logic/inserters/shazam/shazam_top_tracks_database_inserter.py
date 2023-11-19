from typing import List, Dict

from postgres_client import ShazamTopTrack, ShazamLocation, insert_records

from data_collectors.consts.shazam_consts import KEY
from data_collectors.logic.inserters.base_database_inserter import BaseDatabaseInserter
from data_collectors.logs import logger


class ShazamTopTracksDatabaseInserter(BaseDatabaseInserter):
    async def insert(self, locations_tracks: Dict[ShazamLocation, List[dict]]) -> List[ShazamTopTrack]:
        logger.info("Starting to insert shazam top tracks to database")
        records = self._to_records(locations_tracks)
        await insert_records(engine=self._db_engine, records=records)
        logger.info("Successfully inserted shazam top tracks to database")

        return records

    def _to_records(self, locations_tracks: Dict[ShazamLocation, List[dict]]) -> List[ShazamTopTrack]:
        records = []

        for location, tracks in locations_tracks.items():
            location_records = self._get_location_records(location, tracks)
            records.extend(location_records)

        return records

    @staticmethod
    def _get_location_records(location: ShazamLocation, tracks: List[dict]) -> List[ShazamTopTrack]:
        logger.info(f"Turning `{location.value}` tracks to ORM records")
        records = []

        for i, track in enumerate(tracks):
            record = ShazamTopTrack(
                shazam_id=track[KEY],
                location=location,
                position=i+1
            )
            records.append(record)

        return records
