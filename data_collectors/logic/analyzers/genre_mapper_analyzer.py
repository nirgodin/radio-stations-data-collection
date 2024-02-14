from typing import Dict, Optional, List

from genie_datastores.postgres.models import PrimaryGenre
from nltk import MWETokenizer

from data_collectors.contract import IAnalyzer


class GenreMapperAnalyzer(IAnalyzer):
    def __init__(self, genres_mapping: Dict[str, PrimaryGenre], separator: str = " "):
        self._genres_mapping = genres_mapping
        self._separator = separator

    def analyze(self, genre: str) -> PrimaryGenre:
        tagged_genre = self._genres_mapping.get(genre)

        if tagged_genre is not None:
            return tagged_genre

        primary_genre = self._extract_primary_genre_if_contained(genre)
        return PrimaryGenre.OTHER if primary_genre is None else primary_genre

    def _extract_primary_genre_if_contained(self, genre: str) -> Optional[PrimaryGenre]:
        raw_tokens = genre.split(self._separator)
        tokenized_genre = self._tokenizer.tokenize(raw_tokens)

        for main_genre in self._primary_genres:
            for token in tokenized_genre:
                if token == main_genre.value:
                    return main_genre

    @property
    def _tokenizer(self) -> MWETokenizer:
        tokenizer = MWETokenizer(separator=self._separator)

        for genre in self._primary_genres:
            if self._is_multi_word_expression(genre.value):
                tokenizer_formatted_genre = tuple(genre.value.split(self._separator))
                tokenizer.add_mwe(tokenizer_formatted_genre)

        return tokenizer

    @property
    def _primary_genres(self) -> List[PrimaryGenre]:
        unique_genres = {genre.value for genre in self._genres_mapping.values()}
        sorted_genres = sorted(unique_genres)

        return [PrimaryGenre(genre) for genre in sorted_genres]

    def _is_multi_word_expression(self, s: str) -> bool:
        return len(s.split(self._separator)) > 1
