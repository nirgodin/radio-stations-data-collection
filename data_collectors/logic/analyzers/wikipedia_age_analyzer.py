import os
import re
from copy import deepcopy
from datetime import datetime
from re import Pattern
from typing import Dict, List, Tuple, Optional

from genie_common.tools import SyncPoolExecutor, logger
from genie_common.utils import (
    search_between_two_characters,
    contains_any_hebrew_character,
    to_datetime,
    read_json,
)

from data_collectors.consts.wikipedia_consts import WIKIPEDIA_DATETIME_FORMATS
from data_collectors.contract import IAnalyzer
from data_collectors.logic.models import ArtistWikipediaDetails


class WikipediaAgeAnalyzer(IAnalyzer):
    def __init__(self, pool_executor: SyncPoolExecutor = SyncPoolExecutor()):
        self._pool_executor = pool_executor
        self._punctuation_regex: Pattern = re.compile(r"[^A-Za-z0-9]+")

    def analyze(self, summaries: Dict[str, str]) -> List[ArtistWikipediaDetails]:
        logger.info(
            f"Starting to extract age details from Wikipedia page for {len(summaries)} artists"
        )
        return self._pool_executor.run(
            iterable=summaries.items(),
            func=self._extract_single_artist_age_details,
            expected_type=ArtistWikipediaDetails,
        )

    def _extract_single_artist_age_details(
        self, artist_id_and_summary: Tuple[str, str]
    ) -> ArtistWikipediaDetails:
        artist_id, summary = artist_id_and_summary
        birth_date, death_date = self._get_birth_and_death_date(summary)

        return ArtistWikipediaDetails(
            id=artist_id, birth_date=birth_date, death_date=death_date
        )

    def _get_birth_and_death_date(
        self, page_summary: str
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        birth_date = self._extract_normalized_birth_date(page_summary)
        if birth_date:
            return birth_date, None

        return self._extract_normalized_birth_and_death_date(page_summary)

    def _extract_normalized_birth_and_death_date(
        self, page_summary: str
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        raw_dates = search_between_two_characters(
            start_char=r"\(", end_char=r"\)", text=page_summary
        )
        if not raw_dates:
            return None, None

        first_match = raw_dates[0]
        split_dates = first_match.split("–")

        if len(split_dates) == 2:
            dates = [self._extract_date(date) for date in split_dates]
            return dates[0], dates[1]

        return None, None

    def _extract_normalized_birth_date(self, page_summary: str) -> Optional[datetime]:
        raw_birth_date = search_between_two_characters(
            start_char=r"(ב\-|né|born on|born|b\.)", end_char=r"\)", text=page_summary
        )

        if raw_birth_date:
            raw_results = raw_birth_date[0]
            return self._extract_date(raw_results[-1])

    def _extract_date(self, raw_date: str) -> Optional[datetime]:
        if contains_any_hebrew_character(raw_date):
            stripped_date = self._extract_hebrew_date(raw_date)

        else:
            split_date = raw_date.split(";")
            clean_date = self._punctuation_regex.sub(" ", split_date[-1])
            stripped_date = clean_date.strip()

        return to_datetime(stripped_date, WIKIPEDIA_DATETIME_FORMATS)

    def _extract_hebrew_date(self, raw_date: str) -> str:
        modified_date = deepcopy(raw_date)

        for hebrew_month, english_month in self._hebrew_months_mapping.items():
            modified_date = modified_date.replace(hebrew_month, english_month)

            if modified_date != raw_date:
                break

        return self._punctuation_regex.sub(" ", modified_date).strip()

    @property
    def _hebrew_months_mapping(self) -> Dict[str, str]:
        dir_path = os.path.dirname(__file__)
        file_path = os.path.join(dir_path, "hebrew_months_mapping.json")

        return read_json(file_path)
