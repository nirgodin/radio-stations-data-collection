from datetime import datetime
from typing import Iterable, Type, List, Optional

from genie_common.utils import to_datetime, safe_nested_get
from genie_datastores.postgres.models import ShazamArtist

from data_collectors.consts.datetime_consts import SHAZAM_DATETIME_FORMATS
from data_collectors.consts.shazam_consts import (
    ATTRIBUTES,
    BORN_OR_FORMED,
    VIEWS,
    SIMILAR_ARTISTS,
    DATA,
    GENRE_NAMES,
    ORIGIN,
)
from data_collectors.consts.spotify_consts import ID, NAME
from data_collectors.logic.inserters.postgres.base_ids_database_inserter import (
    BaseIDsDatabaseInserter,
)


class ShazamArtistsPostgresDatabaseInserter(BaseIDsDatabaseInserter):
    async def _get_raw_records(self, iterable: Iterable[dict]) -> Iterable[dict]:
        return iterable

    def _to_records(self, raw_records: Iterable[dict]) -> List[ShazamArtist]:
        serialized_records = []

        for raw_record in raw_records:
            record = self._to_record(raw_record)

            if isinstance(record, ShazamArtist):
                serialized_records.append(record)

        return serialized_records

    def _to_record(self, response: dict) -> Optional[ShazamArtist]:
        artist = self._extract_artist_from_response(response)
        if artist:
            return ShazamArtist(
                id=artist[ID],
                name=safe_nested_get(artist, [ATTRIBUTES, NAME]),
                birth_date=self._extract_birth_date(artist),
                genres=safe_nested_get(artist, [ATTRIBUTES, GENRE_NAMES]),
                origin=safe_nested_get(artist, [ATTRIBUTES, ORIGIN]),
                similar_artists=self._extract_related_artists_ids(artist),
            )

    @staticmethod
    def _extract_artist_from_response(response: dict) -> Optional[dict]:
        data = response.get(DATA)

        if data:
            return data[0]

    @staticmethod
    def _extract_birth_date(artist: dict) -> Optional[datetime]:
        birth_date = safe_nested_get(artist, [ATTRIBUTES, BORN_OR_FORMED])

        if birth_date is not None:
            return to_datetime(birth_date, SHAZAM_DATETIME_FORMATS)

    @staticmethod
    def _extract_related_artists_ids(artist: dict) -> Optional[List[str]]:
        similar_artists_records = safe_nested_get(artist, [VIEWS, SIMILAR_ARTISTS, DATA])

        if similar_artists_records:
            return [record[ID] for record in similar_artists_records]

    @property
    def _orm(self) -> Type[ShazamArtist]:
        return ShazamArtist

    @property
    def name(self) -> str:
        return "shazam_artists"
