from typing import Any, List, Optional, Tuple

from async_lru import alru_cache
from genie_common.tools import AioPoolExecutor, logger
from genie_common.utils import safe_nested_get
from genie_datastores.postgres.models import ChartEntry, SpotifyTrack
from genie_datastores.postgres.operations import execute_query
from spotipyio import SpotifyClient, SearchItem, SearchItemMetadata, SpotifySearchType, MatchingEntity, EntityMatcher
from spotipyio.utils import extract_first_search_result
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.spotify_consts import ID, TRACK, TRACKS, ITEMS
from data_collectors.contract import ICollector
from data_collectors.logic.models.radio_chart_entry_details import RadioChartEntryDetails


class ChartsTracksCollector(ICollector):
    def __init__(self, pool_executor: AioPoolExecutor, db_engine: AsyncEngine, spotify_client: SpotifyClient, entity_matcher: EntityMatcher = EntityMatcher()):
        self._pool_executor = pool_executor
        self._db_engine = db_engine
        self._spotify_client = spotify_client
        self._entity_matcher = entity_matcher

    async def collect(self, charts_entries: List[ChartEntry]) -> Any:
        logger.info(f"Starting to collect tracks for {len(charts_entries)} charts entries")
        return await self._pool_executor.run(
            iterable=charts_entries,
            func=self._create_single_chart_entry_details,
            expected_type=RadioChartEntryDetails
        )

    async def _create_single_chart_entry_details(self, chart_entry: ChartEntry) -> RadioChartEntryDetails:
        if chart_entry.track_id is None:
            return await self._create_chart_entry_details_by_key(chart_entry)

        return await self._create_chart_entry_details_by_track_id(chart_entry)

    async def _create_chart_entry_details_by_key(self, chart_entry: ChartEntry) -> RadioChartEntryDetails:
        track_id = await self._query_existing_tracks_by_key(chart_entry)

        if track_id is None:
            return await self._create_non_existing_chart_entry(chart_entry)

        chart_entry.track_id = track_id
        return RadioChartEntryDetails(entry=chart_entry)

    async def _query_existing_tracks_by_key(self, chart_entry: ChartEntry) -> Optional[str]:
        query = (
            select(ChartEntry.track_id)
            .where(ChartEntry.chart == chart_entry.chart)
            .where(ChartEntry.key == chart_entry.key)
            .limit(1)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().first()

    async def _create_non_existing_chart_entry(self, chart_entry: ChartEntry) -> Optional[RadioChartEntryDetails]:
        search_result = await self._search_track(chart_entry.key)
        track = self._extract_matching_track(chart_entry, search_result)

        if track is not None:
            chart_entry.track_id = track[ID]
            track = {TRACK: track}

        return RadioChartEntryDetails(
            entry=chart_entry,
            track=track
        )

    @alru_cache(maxsize=1000)
    async def _search_track(self, text: str) -> dict:
        search_item = SearchItem(
            text=text,
            metadata=SearchItemMetadata(
                search_types=[SpotifySearchType.TRACK],
                quote=False
            )
        )
        return await self._spotify_client.search.run_single(search_item)

    def _extract_matching_track(self, chart_entry: ChartEntry, search_result: dict) -> Optional[dict]:
        items = safe_nested_get(search_result, [TRACKS, ITEMS], [])

        for candidate in items:
            artist, track = self._extract_artist_and_track_from_chart_key(chart_entry.key)
            entity = MatchingEntity(
                track=track.strip(),
                artist=artist.strip()
            )
            is_matching, score = self._entity_matcher.match(entity, candidate)

            if is_matching:
                return candidate

        self._log_track_not_found(chart_entry, match_field="key")

    @staticmethod
    def _extract_artist_and_track_from_chart_key(key: str) -> Tuple[str, str]:
        key_components = key.split("-")
        n_component = len(key_components)

        if n_component < 2:
            return key, ""
        if n_component == 2:
            return key_components[0].strip(), key_components[1].strip()

        return key_components[0].strip(), "-".join(key_components[1:]).strip()

    @staticmethod
    def _log_track_not_found(chart_entry: ChartEntry, match_field: str) -> None:
        stringified_date = chart_entry.date.strftime("%d-%m-%Y")
        match_value = getattr(chart_entry, match_field)
        logger.info(f"Did not find any track that matches `{match_value}` from date `{stringified_date}`. Ignoring")

    async def _create_chart_entry_details_by_track_id(self, chart_entry: ChartEntry) -> RadioChartEntryDetails:
        is_existing_track_id = await self._is_existing_track_id(chart_entry.track_id)
        if is_existing_track_id:
            return RadioChartEntryDetails(entry=chart_entry)

        tracks = await self._spotify_client.tracks.run([chart_entry.track_id])

        if tracks:
            return RadioChartEntryDetails(
                entry=chart_entry,
                track={TRACK: tracks[0]}
            )
        else:
            self._log_track_not_found(chart_entry, match_field="track_id")

    async def _is_existing_track_id(self, track_id: str) -> bool:
        query = (
            select(SpotifyTrack.id)
            .where(SpotifyTrack.id == track_id)
            .limit(1)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)
        database_id = query_result.scalars().first()

        return database_id is not None
