from abc import abstractmethod, ABC
from functools import lru_cache
from typing import List, Dict, Tuple, Optional

from genie_common.tools import AioPoolExecutor, logger
from sqlalchemy.ext.asyncio import AsyncEngine
from wikipediaapi import Wikipedia

from data_collectors.logic.collectors.wikipedia.wikipedia_page_summary_collector import (
    WikipediaPageSummaryCollector,
)


class BaseWikipediaAgeCollector(ABC):
    def __init__(
        self,
        db_engine: AsyncEngine,
        pool_executor: AioPoolExecutor,
        page_summary_collector: WikipediaPageSummaryCollector = WikipediaPageSummaryCollector(),
    ):
        self._db_engine = db_engine
        self._pool_executor = pool_executor
        self._page_summary_collector = page_summary_collector

    async def collect(self, limit: Optional[int]) -> Dict[str, str]:
        logger.info(f"Starting to collect artists age details using `{self.__class__.__name__}` collector")
        artists_ids_and_details = await self._get_missing_artists_details(limit)
        results = await self._pool_executor.run(
            iterable=artists_ids_and_details,
            func=self._collect_single_artist_summary,
            expected_type=tuple,
        )

        return dict(results)

    @abstractmethod
    async def _get_missing_artists_details(self, limit: Optional[int]) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def _get_artist_wikipedia_name(self, artist_detail: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def _get_wikipedia_abbreviation(self, artist_detail: str) -> str:
        raise NotImplementedError

    async def _collect_single_artist_summary(self, artist_id_and_detail: str) -> Tuple[str, str]:
        artist_id, artist_detail = artist_id_and_detail
        artist_page_name = self._get_artist_wikipedia_name(artist_detail)
        wikipedia_abbreviation = self._get_wikipedia_abbreviation(artist_detail)
        summary = await self._page_summary_collector.collect(artist_page_name, wikipedia_abbreviation)

        return artist_id, summary

    @lru_cache
    def _get_wikipedia(self, language: str) -> Wikipedia:
        return Wikipedia(language)
