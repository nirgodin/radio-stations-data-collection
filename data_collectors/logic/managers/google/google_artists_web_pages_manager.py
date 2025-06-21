from datetime import datetime
from typing import Optional, Dict, List
from urllib.parse import urlparse, ParseResult, unquote

from genie_common.tools import logger
from genie_datastores.postgres.models import SpotifyArtist
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.logic.updaters import ValuesDatabaseUpdater
from data_collectors.contract import IManager
from data_collectors.logic.collectors import GoogleArtistsWebPagesCollector
from data_collectors.logic.models import DBUpdateRequest, DomainExtractSettings


class GoogleArtistsWebPagesManager(IManager):
    def __init__(
        self,
        db_engine: AsyncEngine,
        web_pages_collector: GoogleArtistsWebPagesCollector,
        db_updater: ValuesDatabaseUpdater,
    ):
        self._db_engine = db_engine
        self._web_pages_collector = web_pages_collector
        self._db_updater = db_updater

    async def run(self, limit: Optional[int]) -> None:
        ids_to_artists_map = await self._query_artists_with_missing_web_pages(limit)
        artists_ids_names_map = {artist.id: artist.name for artist in ids_to_artists_map.values()}
        artist_id_web_pages_map = await self._web_pages_collector.collect(artists_ids_names_map)
        update_requests = self._to_update_requests(ids_to_artists_map, artist_id_web_pages_map)

        await self._db_updater.update(update_requests)

    async def _query_artists_with_missing_web_pages(self, limit: Optional[int]) -> Dict[str, SpotifyArtist]:
        logger.info(f"Querying {limit} artists with missing web pages")
        query = (
            select(SpotifyArtist)
            .where(SpotifyArtist.wikipedia_name.is_(None))
            .order_by(SpotifyArtist.update_date.asc())
            .limit(limit)
        )
        query_result = await execute_query(self._db_engine, query)

        return {row.id: row for row in query_result.scalars().all()}

    def _to_update_requests(
        self, ids_artists_map: Dict[str, SpotifyArtist], ids_web_pages_map: Dict[str, Dict[str, str]]
    ) -> List[DBUpdateRequest]:
        logger.info(f"Building {len(ids_web_pages_map)} update requests")
        requests = []

        for artist_id, web_pages in ids_web_pages_map.items():
            artist = ids_artists_map[artist_id]
            update_request = self._to_update_request(artist, web_pages)
            requests.append(update_request)

        return requests

    def _to_update_request(self, artist: SpotifyArtist, web_pages: Dict[str, str]) -> DBUpdateRequest:
        update_values = {SpotifyArtist.update_date: datetime.utcnow()}

        for domain, link in web_pages.items():
            parse_result = urlparse(link)
            extract_settings = self._domain_extract_function_map[domain]

            for setting in extract_settings:
                if self._is_missing_column(setting, artist):
                    update_values[setting.column] = setting.extract_fn(parse_result)

        return DBUpdateRequest(id=artist.id, values=update_values)

    @staticmethod
    def _is_missing_column(extract_settings: DomainExtractSettings, artist: SpotifyArtist) -> bool:
        existing_value = getattr(artist, extract_settings.column.key)
        return existing_value is None

    @staticmethod
    def _extract_wiki_name(parse_result: ParseResult) -> str:
        formatted_path = parse_result.path.replace("wiki", "").strip("/")
        return unquote(formatted_path)

    @staticmethod
    def _extract_wiki_language(parse_result: ParseResult) -> str:
        return parse_result.hostname.split(".")[0]

    @staticmethod
    def _extract_domain_route(parse_result: ParseResult) -> str:
        return parse_result.path.strip("/")

    @property
    def _domain_extract_function_map(self) -> Dict[str, List[DomainExtractSettings]]:
        domain_extract_map = {}

        for domain_extract in self._domain_extract_functions:
            if domain_extract.domain not in domain_extract_map.keys():
                domain_extract_map[domain_extract.domain] = []

            domain_extract_map[domain_extract.domain].append(domain_extract)

        return domain_extract_map

    @property
    def _domain_extract_functions(self) -> List[DomainExtractSettings]:
        return [
            DomainExtractSettings(
                domain="wikipedia",
                extract_fn=self._extract_wiki_name,
                column=SpotifyArtist.wikipedia_name,
            ),
            DomainExtractSettings(
                domain="wikipedia",
                extract_fn=self._extract_wiki_language,
                column=SpotifyArtist.wikipedia_language,
            ),
            DomainExtractSettings(
                domain="facebook",
                extract_fn=self._extract_domain_route,
                column=SpotifyArtist.facebook_name,
            ),
            DomainExtractSettings(
                domain="instagram",
                extract_fn=self._extract_domain_route,
                column=SpotifyArtist.instagram_name,
            ),
            DomainExtractSettings(
                domain="twitter",
                extract_fn=self._extract_domain_route,
                column=SpotifyArtist.twitter_name,
            ),
        ]
