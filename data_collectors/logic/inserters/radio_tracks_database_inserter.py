from typing import List

from genie_datastores.postgres.operations import insert_records, execute_query
from genie_datastores.postgres.models import RadioTrack
from genie_datastores.postgres.inner_utils.spotify_utils import extract_artist_id
from sqlalchemy import tuple_, select

from data_collectors.consts.spotify_consts import TRACK, ID
from data_collectors.contract.base_database_inserter import BaseDatabaseInserter
from data_collectors.logs import logger


class RadioTracksDatabaseInserter(BaseDatabaseInserter):
    async def insert(self, playlist: dict, tracks: List[dict], artists: List[dict]) -> None:
        records = await self._to_records(playlist=playlist, tracks=tracks, artists=artists)
        non_existing_records = await self._filter_out_existing_records(records)

        if non_existing_records:
            logger.info(f"Inserting {len(non_existing_records)} record to radio_tracks table")
            await insert_records(engine=self._db_engine, records=non_existing_records)
        else:
            logger.info("Did not find any new record to insert. Skipping.")

    async def _to_records(self, playlist: dict, tracks: List[dict], artists: List[dict]) -> List[RadioTrack]:
        records = []

        for track in tracks:
            artist = self._extract_artist_details(track, artists)
            record = RadioTrack.from_playlist_artist_track(
                playlist=playlist,
                artist=artist,
                track=track
            )
            records.append(record)

        return records

    @staticmethod
    def _extract_artist_details(track: dict, artists: List[dict]) -> dict:
        artist_id = extract_artist_id(track[TRACK])

        for artist in artists:
            if artist[ID] == artist_id:
                return artist

    async def _filter_out_existing_records(self, records: List[RadioTrack]) -> List[RadioTrack]:
        non_existing_records = []
        existing_records = await self._get_existing_records(records)

        for record in records:
            if self._is_new_record(record, existing_records):
                non_existing_records.append(record)
            else:
                logger.warning(
                    f"Record from station `{record.station}` with track id `{record.track_id}` already exist. Skipping"
                )

        return non_existing_records

    async def _get_existing_records(self, records: List[RadioTrack]) -> List[RadioTrack]:
        filter_columns = tuple_(RadioTrack.track_id, RadioTrack.added_at, RadioTrack.station)
        records_columns = [(record.track_id, record.added_at, record.station) for record in records]
        query = (
            select(RadioTrack)
            .where(filter_columns.in_(records_columns))
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().all()

    def _is_new_record(self, candidate: RadioTrack, existing_records: List[RadioTrack]) -> bool:
        return not any(self._has_same_id_date_and_station(candidate, record) for record in existing_records)

    @staticmethod
    def _has_same_id_date_and_station(candidate: RadioTrack, record: RadioTrack) -> bool:
        has_same_id_and_date = candidate.track_id == record.track_id and candidate.added_at == record.added_at
        return has_same_id_and_date and candidate.station == record.station
