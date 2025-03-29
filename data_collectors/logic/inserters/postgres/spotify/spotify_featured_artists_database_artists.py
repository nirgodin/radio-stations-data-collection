from typing import List, Type

from genie_common.utils import chain_lists
from genie_datastores.postgres.models import SpotifyFeaturedArtist
from spotipyio.logic.utils import safe_nested_get

from data_collectors.consts.spotify_consts import ID, TRACK
from data_collectors.logic.inserters.postgres.spotify.base_spotify_database_inserter import (
    BaseSpotifyDatabaseInserter,
)
from data_collectors.logic.models import FeaturedArtist
from data_collectors.utils.spotify import get_track_artists


class SpotifyFeaturedArtistsDatabaseInserter(BaseSpotifyDatabaseInserter):
    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        featured_artists = [self._to_featured_artists(track) for track in tracks]
        chained_featured_artists: List[FeaturedArtist] = chain_lists(featured_artists)

        return [artist.dict() for artist in chained_featured_artists]

    @property
    def name(self) -> str:
        return "featured_artists"

    @property
    def _orm(self) -> Type[SpotifyFeaturedArtist]:
        return SpotifyFeaturedArtist

    @staticmethod
    def _to_featured_artists(track: dict) -> List[FeaturedArtist]:
        track_id = safe_nested_get(track, [TRACK, ID])
        if track_id is None:
            return []

        raw_artists = get_track_artists(track)
        raw_featured_artists = raw_artists[1:]

        return [FeaturedArtist.from_raw_artist(track_id, i, artist) for i, artist in enumerate(raw_featured_artists)]
