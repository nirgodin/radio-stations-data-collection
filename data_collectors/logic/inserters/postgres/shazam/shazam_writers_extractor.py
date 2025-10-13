import re
from typing import List, Optional, Generator

from data_collectors.consts.shazam_consts import LYRICS_FOOTER, SECTIONS, TYPE, SHAZAM_LYRICS


class ShazamWritersExtractor:
    @staticmethod
    def extract(response: dict) -> Optional[List[str]]:
        shazam_lyrics_section = ShazamWritersExtractor._extract_shazam_lyrics_section(response)
        footer = shazam_lyrics_section.get(LYRICS_FOOTER)

        if footer:
            return ShazamWritersExtractor._extract_writers_from_footer(footer)

    @staticmethod
    def _extract_shazam_lyrics_section(response: dict) -> dict:
        for section in response.get(SECTIONS, []):
            section_type = section.get(TYPE, "")

            if section_type == SHAZAM_LYRICS:
                return section

        return {}

    @staticmethod
    def _extract_writers_from_footer(footer: str) -> Optional[List[str]]:
        relevant_footer = footer.split("\n")[0]
        split_footer = relevant_footer.split(":")

        if len(split_footer) >= 2:
            return list(ShazamWritersExtractor._generate_valid_writers_names(split_footer))

    @staticmethod
    def _generate_valid_writers_names(split_footer: List[str]) -> Generator[str, None, None]:
        writers_element = split_footer[1]

        for writer in writers_element.split(","):
            if ShazamWritersExtractor._is_valid_writer(writer):
                yield ShazamWritersExtractor._pre_process_raw_writer(writer)

    @staticmethod
    def _is_valid_writer(writer: str) -> bool:
        return any(letter.isalpha() for letter in writer)

    @staticmethod
    def _pre_process_raw_writer(raw_writer: str) -> str:
        stripped_writer = raw_writer.strip()
        return re.sub(r" +", " ", stripped_writer)
