import re
import string
from collections import Counter
from functools import partial
from typing import List, Dict, Tuple

from genie_common.tools import SyncPoolExecutor
from genie_common.utils import sort_dict_by_value
from genie_datastores.postgres.models import TrackLyrics, TrackIDMapping
from langid.langid import LanguageIdentifier
from sqlalchemy.engine import Row

from data_collectors.contract import ISerializer
from data_collectors.logic.models import LyricsSourceDetails


class TracksLyricsSerializer(ISerializer):
    def __init__(
        self,
        language_identifier: LanguageIdentifier,
        pool_executor: SyncPoolExecutor = SyncPoolExecutor(),
    ):
        self._language_identifier = language_identifier
        self._pool_executor = pool_executor
        self._numeric_punctuation_spaces_regex = re.compile(r"[%s]" % re.escape(string.punctuation))

    def serialize(
        self,
        ids_lyrics_mapping: Dict[str, List[str]],
        source_details: LyricsSourceDetails,
        track_ids_mapping: List[Row],
    ) -> List[TrackLyrics]:
        func = partial(self._serialize_single_track_lyrics, source_details, track_ids_mapping)
        return self._pool_executor.run(iterable=ids_lyrics_mapping.items(), func=func, expected_type=TrackLyrics)

    def _serialize_single_track_lyrics(
        self,
        source_details: LyricsSourceDetails,
        track_ids_mapping: List[Row],
        source_id_and_lyrics: Tuple[str, List[str]],
    ) -> TrackLyrics:
        source_id, lyrics = source_id_and_lyrics
        spotify_id = self._find_spotify_id(
            source_id=source_id,
            column=source_details.column,
            track_ids_mapping=track_ids_mapping,
        )
        joined_lyrics = "\n".join(lyrics)
        language, confidence = self._language_identifier.classify(joined_lyrics)
        words_count = self._compute_words_count(lyrics)

        return TrackLyrics(
            id=spotify_id,
            lyrics=lyrics,
            lyrics_source=source_details.data_source,
            language=language,
            language_confidence=confidence,
            number_of_words=sum(words_count.values()),
            words_count=words_count,
        )

    @staticmethod
    def _find_spotify_id(source_id: str, column: TrackIDMapping, track_ids_mapping: List[Row]) -> str:
        for row in track_ids_mapping:
            row_source_id = getattr(row, column.key)

            if source_id == row_source_id:
                return row.id

    def _compute_words_count(self, lyrics: List[str]) -> Dict[str, float]:
        track_word_count = Counter()

        for line in lyrics:
            clean_line = self._numeric_punctuation_spaces_regex.sub("", line)
            tokens = [token.strip().lower() for token in clean_line.split(" ") if token != ""]
            tokens_count = Counter(tokens)
            track_word_count += tokens_count

        return sort_dict_by_value(track_word_count)
