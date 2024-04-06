from datetime import datetime
from functools import partial
from typing import Any, Tuple, List, Dict, Optional

from genie_common.tools import SyncPoolExecutor, logger
from genie_common.utils import safe_nested_get
from spotipyio import SpotifyClient, EntityMatcher, MatchingEntity

from data_collectors.consts.spotify_consts import ID, ITEMS, TRACKS, TRACK
from data_collectors.contract import ICollector
from data_collectors.logic.models import EurovisionRecord
from data_collectors.utils.charts import extract_artist_and_track_from_chart_key


class EurovisionMissingTracksCollector(ICollector):
    def __init__(self,
                 spotify_client: SpotifyClient,
                 pool_executor: SyncPoolExecutor = SyncPoolExecutor(),
                 entity_matcher: EntityMatcher = EntityMatcher()):
        self._spotify_client = spotify_client
        self._pool_executor = pool_executor
        self._entity_matcher = entity_matcher

    async def collect(self, records: List[EurovisionRecord]) -> Dict[int, dict]:
        logger.info("Starting to collecting missing entries tracks ids")
        years_playlists = await self._fetch_eurovision_years_playlists()
        logger.info("Starting to match missing records to Eurovision playlists tracks")
        tracks = self._pool_executor.run(
            iterable=records,
            func=partial(self._match_single_track_id, years_playlists),
            expected_type=tuple
        )

        return dict(tracks)

    async def _fetch_eurovision_years_playlists(self) -> Dict[int, Dict[str, Any]]:
        logger.info("Starting to fetch eurovision playlists")
        ids = list(self._eurovision_playlists.keys())
        playlists = await self._spotify_client.playlists.info.run(ids)

        return self._map_years_to_playlists(playlists)

    def _map_years_to_playlists(self, playlists: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
        year_playlist_mapping = {}

        for playlist in playlists:
            playlist_id = playlist[ID]
            playlist_year = self._eurovision_playlists[playlist_id]
            year_playlist_mapping[playlist_year] = playlist

        return year_playlist_mapping

    def _match_single_track_id(self, years_playlists: Dict[int, dict], record: EurovisionRecord) -> Tuple[int, dict]:
        playlist = years_playlists[record.date.year]
        items = safe_nested_get(playlist, [TRACKS, ITEMS], [])
        artist, track = extract_artist_and_track_from_chart_key(record.key)
        entity = MatchingEntity(
            track=track.strip(),
            artist=artist.strip()
        )
        matching_item = self._extract_matching_track_id(items, entity)

        if matching_item is not None:
            return record.id, matching_item

    def _extract_matching_track_id(self, items: List[Dict[str, Any]], entity: MatchingEntity) -> Optional[dict]:
        for item in items:
            candidate = item.get(TRACK)

            if candidate is not None:
                is_matching, score = self._entity_matcher.match(entity, candidate)

                if is_matching:
                    return item

    @property
    def _eurovision_playlists(self) -> Dict[str, int]:
        return {
            "4Z3QlZfwhCNKGhqsiDi1oH": 1990,
            "09OKlgunTlKElpxU6RHMQr": 2013,
            "3svTn0yv1hfv8vzMzVd31D": 2014,
            "1HtXxHLxLvC5ePfd0hQC7U": 2015,
            "0VEtwmjx3FK77jWLlI16EV": 2016,
            "6Ey20ccpxRCd0AlaIuGJrB": 2017,
            "1LiCfqzUjGcGlFsJJLzPD0": 2019,
            "5h0sQpJnLVzgy5iOH1UNcl": 2021,
        }
