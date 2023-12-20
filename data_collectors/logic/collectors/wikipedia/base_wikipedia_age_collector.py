import copy
import os.path
import re
from abc import abstractmethod, ABC
from datetime import datetime
from functools import partial, lru_cache
from typing import List, Dict, Tuple, Optional

from genie_common.tools import AioPoolExecutor, logger
from genie_common.utils import to_datetime, run_async, contains_any_hebrew_character, search_between_two_characters, \
    read_json
from sqlalchemy.ext.asyncio import AsyncEngine
from wikipediaapi import Wikipedia

from data_collectors.consts.wikipedia_consts import WIKIPEDIA_DATETIME_FORMATS
from data_collectors.logic.models import ArtistWikipediaDetails


class BaseWikipediaAgeCollector(ABC):
    def __init__(self, db_engine: AsyncEngine, pool_executor: AioPoolExecutor):
        self._db_engine = db_engine
        self._pool_executor = pool_executor
        self._punctuation_regex = re.compile(r'[^A-Za-z0-9]+')

    async def collect(self, limit: Optional[int]) -> List[ArtistWikipediaDetails]:
        logger.info(f"Starting to collect artists age details using `{self.__class__.__name__}` collector")
        artists_ids_and_details = await self._get_missing_artists_details(limit)
        return await self._pool_executor.run(
            iterable=artists_ids_and_details,
            func=self._collect_single_artist_age,
            expected_type=ArtistWikipediaDetails
        )

    @abstractmethod
    async def _get_missing_artists_details(self, limit: Optional[int]) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def _get_artist_wikipedia_name(self, artist_detail: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def _get_wikipedia_abbreviation(self, artist_detail: str) -> str:
        raise NotImplementedError

    async def _collect_single_artist_age(self, artist_id_and_detail: str) -> ArtistWikipediaDetails:
        artist_id, artist_detail = artist_id_and_detail
        artist_page_name = self._get_artist_wikipedia_name(artist_detail)
        wikipedia_abbreviation = self._get_wikipedia_abbreviation(artist_detail)
        wikipedia = self._get_wikipedia(wikipedia_abbreviation)
        func = partial(wikipedia.page, artist_page_name)
        page = await run_async(func)
        birth_date, death_date = self._get_birth_and_death_date(page.summary)

        return ArtistWikipediaDetails(
            id=artist_id,
            birth_date=birth_date,
            death_date=death_date
        )

    def _get_birth_and_death_date(self, page_summary: str) -> Tuple[Optional[datetime], Optional[datetime]]:
        birth_date = self._extract_normalized_birth_date(page_summary)
        if birth_date:
            return birth_date, None

        return self._extract_normalized_birth_and_death_date(page_summary)

    def _extract_normalized_birth_date(self, page_summary: str) -> Optional[datetime]:
        raw_birth_date = search_between_two_characters(
            start_char=r'(ב\-|né|born on|born|b\.)',
            end_char=r'\)',
            text=page_summary
        )

        if raw_birth_date:
            raw_results = raw_birth_date[0]
            return self._extract_date(raw_results[-1])

    def _extract_normalized_birth_and_death_date(self, page_summary: str) -> Tuple[Optional[datetime], Optional[datetime]]:
        raw_dates = search_between_two_characters(
            start_char=r'\(',
            end_char=r'\)',
            text=page_summary
        )
        if not raw_dates:
            return None, None

        first_match = raw_dates[0]
        split_dates = first_match.split('–')

        if len(split_dates) == 2:
            dates = [self._extract_date(date) for date in split_dates]
            return dates[0], dates[1]

        return None, None

    def _extract_date(self, raw_date: str) -> Optional[datetime]:
        if contains_any_hebrew_character(raw_date):
            stripped_date = self._extract_hebrew_date(raw_date)

        else:
            split_date = raw_date.split(';')
            clean_date = self._punctuation_regex.sub(' ', split_date[-1])
            stripped_date = clean_date.strip()

        return to_datetime(stripped_date, WIKIPEDIA_DATETIME_FORMATS)

    def _extract_hebrew_date(self, raw_date: str) -> str:
        modified_date = copy.deepcopy(raw_date)

        for hebrew_month, english_month in self._hebrew_months_mapping.items():
            modified_date = modified_date.replace(hebrew_month, english_month)

            if modified_date != raw_date:
                break

        return self._punctuation_regex.sub(' ', modified_date).strip()

    @lru_cache
    def _get_wikipedia(self, language: str) -> Wikipedia:
        return Wikipedia(language)

    @property
    def _hebrew_months_mapping(self) -> Dict[str, str]:
        dir_path = os.path.dirname(__file__)
        file_path = os.path.join(dir_path, "hebrew_months_mapping.json")

        return read_json(file_path)
