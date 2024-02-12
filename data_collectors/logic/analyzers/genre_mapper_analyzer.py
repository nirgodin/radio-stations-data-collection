from typing import Dict, Optional, List

import pandas as pd
from genie_datastores.postgres.models import PrimaryGenre
from nltk import MWETokenizer

from data_collectors.contract import IAnalyzer


class GenreMapperAnalyzer(IAnalyzer):
    def __init__(self, separator: str = " "):
        self._separator = separator

    def analyze(self, genre: str) -> PrimaryGenre:
        if self._is_genre_tagged(genre):
            return self._tagged_genres[genre]

        primary_genre = self._extract_main_genre_if_contained(genre)
        return PrimaryGenre.OTHER if primary_genre is None else primary_genre

    def _is_genre_tagged(self, genre: str) -> bool:
        return genre in self._tagged_genres.keys()

    def _extract_main_genre_if_contained(self, genre: str) -> Optional[PrimaryGenre]:
        raw_tokens = genre.split(self._separator)
        tokenized_genre = self._tokenizer.tokenize(raw_tokens)

        for main_genre in self._main_genres:
            for token in tokenized_genre:
                if token == main_genre.value:
                    return main_genre

    @property
    def _tagged_genres(self) -> Dict[str, PrimaryGenre]:
        genres_data = pd.read_csv(r'C:\Users\nirgo\Documents\GitHub\RadioStations\data\resources\genres_labels.csv').fillna('')  # TODO: Convert to JSON on drive
        genres_mapping = {}

        for genre, main_genre in zip(genres_data["genre"], genres_data["main_genre"]):
            if main_genre != '':
                genres_mapping[genre] = PrimaryGenre(main_genre.lower())

        return genres_mapping

    @property
    def _main_genres(self) -> List[PrimaryGenre]:
        main_genres = set(self._tagged_genres.values())
        return list(sorted(main_genres, key=lambda x: x.value))

    @property
    def _tokenizer(self) -> MWETokenizer:
        tokenizer = MWETokenizer(separator=self._separator)

        for genre in self._main_genres:
            if self._is_multi_word_expression(genre.value):
                tokenizer_formatted_genre = tuple(genre.value.split(self._separator))
                tokenizer.add_mwe(tokenizer_formatted_genre)

        return tokenizer

    def _is_multi_word_expression(self, s: str) -> bool:
        return len(s.split(self._separator)) > 1
