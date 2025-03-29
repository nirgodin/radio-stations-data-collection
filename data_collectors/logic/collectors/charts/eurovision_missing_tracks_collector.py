from functools import partial
from typing import Any, Tuple, List, Dict, Optional

from genie_common.tools import SyncPoolExecutor, logger
from genie_common.utils import safe_nested_get
from spotipyio import SpotifyClient
from spotipyio.models import MatchingEntity
from spotipyio.tools.matching import EntityMatcher

from data_collectors.consts.spotify_consts import ID, ITEMS, TRACKS, TRACK
from data_collectors.contract import ICollector
from data_collectors.logic.models import EurovisionRecord
from data_collectors.utils.charts import extract_artist_and_track_from_chart_key


class EurovisionMissingTracksCollector(ICollector):
    def __init__(
        self,
        spotify_client: SpotifyClient,
        pool_executor: SyncPoolExecutor = SyncPoolExecutor(),
        entity_matcher: EntityMatcher = EntityMatcher(),
    ):
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
            expected_type=tuple,
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

    def _match_single_track_id(
        self, years_playlists: Dict[int, dict], record: EurovisionRecord
    ) -> Optional[Tuple[int, dict]]:
        playlist = years_playlists.get(record.date.year)

        if playlist is not None:
            return self._match_entity_to_playlist_items(playlist, record)

        logger.warning(f"Did not find mapped playlists for year {record.date.year}. Skipping record `{record.key}`")

    def _match_entity_to_playlist_items(self, playlist: dict, record: EurovisionRecord) -> Optional[Tuple[int, dict]]:
        items = safe_nested_get(playlist, [TRACKS, ITEMS], [])
        artist, track = extract_artist_and_track_from_chart_key(record.key)
        entity = MatchingEntity(track=track.strip(), artist=artist.strip())
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
            "4n0E0djaAwBRZCoHhCfUDa": 1957,
            "0Ae19Z9vbk4ABZ4vF1EHPm": 1958,
            "6sBcrNhE2Rd9zJJxK729BM": 1959,
            "004P04GKrou1umPr1Fy3Nf": 1960,
            "3QmgDwvHiaSISfjug9I9Bs": 1961,
            "5NiRmfQlMB4Qr1x8iSEFno": 1962,
            "11qkuFKfrbdsPMp2ZwppUD": 1963,
            "2AZ0nckOaUuqPAeEe521Tr": 1964,
            "1nP8JV8h3dmbDACqmLttmo": 1965,
            "05uK2KhbiQZ8j4ykUfrwUC": 1966,
            "3rLDitIA1riJV5eB9GEfV8": 1967,
            "2M7mYzQQcH7a10WfDCYzSz": 1968,
            "1c1lgxoir64Y9VtI2ILdNJ": 1969,
            "6yhQ2hwAfPmfidRWVXpwze": 1970,
            "1eo9uOhiEZ8R61llNrxrSv": 1971,
            "0iCICwJr3O5uBgD47jxjCY": 1972,
            "6R8Hiv4feMpQbTCujqCPab": 1973,
            "7nTlySKOpwMyrWnX58EmbO": 1974,
            "0cp9GgdeMuGQA1KwpbwX6y": 1975,
            "2FSJeYSOGNbxb51fYueybr": 1976,
            "0B7KHwuIiFxEkuG992b9ZB": 1977,
            "3Vl0Q5wHlf9fF9oYnCoXLD": 1978,
            "5ozGGKbULtiJwcK30XGWQs": 1979,
            "1klbuEHhW0t28VKp8QY35b": 1980,
            "6QUP4q0utPXCvzPJpk9d4f": 1981,
            "3wbNoVwoifrN6HtY6ezVCb": 1982,
            "2bYE3tFobQzAZUAOsIHnAs": 1983,
            "1qwQqJagoqiJn3bIYLn6AF": 1984,
            "5FxQ4dJxEDvhGRitvLA0te": 1985,
            "0gZxBB6a4ojUbjqXIJLbU2": 1986,
            "0zfllYLHGTxUy8eKF12qLt": 1987,
            "103GbefXi6douQ6lHAQLkG": 1988,
            "70bqXffF8UyXYZlGBN4bM1": 1989,
            "4Z3QlZfwhCNKGhqsiDi1oH": 1990,
            "5PzHmgNBlRg8G18mjkPybP": 1991,
            "2KsMXkeDSs3vACukhXTYm6": 1992,
            "62BAMbKf81719xi03CS5lI": 1993,
            "3sXZCfRJjq9PeZHQTlGTDS": 1994,
            "7nTbnvuTwgR21tu93xWTG1": 1995,
            "4KmP9ojAaNIi2nlSeqr1TK": 1996,
            "7mheCduINizrNfndQglzBG": 1997,
            "6s8JVzwbvXUccPxKBl964O": 1998,
            "4yVN1ppPhxodJKl8uimKJw": 1999,
            "6twijwW7TCnEOUPLrTFg4U": 2000,
            "7h1GLSuZWJSyJnziShtBQm": 2001,
            "1sdeephZuNhgk1RZkV8efK": 2002,
            "3sxozYHQFvZ9yoZI0irRnA": 2003,
            "5fGHP7oZzm9VE7DOW1a61e": 2004,
            "7gubzwf45FI6Vuz8YqxoAC": 2005,
            "65rVQgIperoJOgcieYBwad": 2006,
            "3b7eCqwUaI15OkKf1ew7bC": 2007,
            "2W6Km8zv5P0Dwro0RJ0d9E": 2008,
            "3st0vXSmuEuQC2QlPMMuc6": 2009,
            "3Vf3E8WavFCRLGF3a97nR9": 2010,
            "4HPdL5jwsxKnh7RTAwq9hF": 2011,
            "5tjy5qbWxaFFFTseRDptC7": 2012,
            "09OKlgunTlKElpxU6RHMQr": 2013,
            "3svTn0yv1hfv8vzMzVd31D": 2014,
            "1HtXxHLxLvC5ePfd0hQC7U": 2015,
            "0VEtwmjx3FK77jWLlI16EV": 2016,
            "6Ey20ccpxRCd0AlaIuGJrB": 2017,
            "6cGLwHvQ1iWb7Hl30kt2c6": 2018,
            "1LiCfqzUjGcGlFsJJLzPD0": 2019,
            "5h0sQpJnLVzgy5iOH1UNcl": 2021,
        }
