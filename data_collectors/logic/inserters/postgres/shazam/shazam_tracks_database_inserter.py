from typing import Iterable, Type, List, Optional

from genie_common.utils import safe_nested_get
from genie_datastores.postgres.inner_utils.spotify_utils import extract_artist_id
from genie_datastores.postgres.models import ShazamTrack

from data_collectors.consts.shazam_consts import ADAM_ID, KEY, TITLE, PRIMARY, LABEL, SECTIONS, METADATA, TEXT
from data_collectors.consts.spotify_consts import GENRES
from data_collectors.logic.inserters.postgres.base_ids_database_inserter import (
    BaseIDsDatabaseInserter,
)
from data_collectors.logic.inserters.postgres.shazam.shazam_writers_extractor import ShazamWritersExtractor


class ShazamTracksDatabaseInserter(BaseIDsDatabaseInserter):
    async def _get_raw_records(self, iterable: Iterable[dict]) -> Iterable[dict]:
        return iterable

    def _to_record(self, response: dict) -> Optional[ShazamTrack]:
        artist_id = extract_artist_id(response, ADAM_ID)
        if artist_id:
            return ShazamTrack(
                id=response[KEY],
                artist_id=artist_id,
                name=response[TITLE],
                primary_genre=safe_nested_get(response, [GENRES, PRIMARY]),
                label=self._extract_metadata_item(response, LABEL),
                writers=ShazamWritersExtractor.extract(response),
            )

    @staticmethod
    def _extract_metadata_item(response: dict, title: str) -> str:
        for section in response.get(SECTIONS, []):
            for item in section.get(METADATA, []):
                item_title = item.get(TITLE, "")

                if item_title == title:
                    return item.get(TEXT)

    @property
    def _orm(self) -> Type[ShazamTrack]:
        return ShazamTrack

    @property
    def name(self) -> str:
        return "shazam_tracks"
