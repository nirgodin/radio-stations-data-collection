from collections import Counter
from typing import Any, List, Tuple, Optional

from genie_common.tools import SyncPoolExecutor
from genie_datastores.postgres.models import PrimaryGenre
from sqlalchemy.engine import Row

from data_collectors.contract import IAnalyzer
from data_collectors.logic.analyzers.genre_mapper_analyzer import GenreMapperAnalyzer


class PrimaryGenreAnalyzer(IAnalyzer):
    def __init__(self,
                 pool_executor: SyncPoolExecutor = SyncPoolExecutor(),
                 genre_mapper: GenreMapperAnalyzer = GenreMapperAnalyzer()):
        self._pool_executor = pool_executor
        self._genre_mapper = genre_mapper

    def analyze(self, rows: List[Row]) -> Any:
        track_to_primary_genre_mapping = self._pool_executor.run(
            iterable=rows,
            func=self._decide_single_track_primary_genre,
            expected_type=tuple
        )
        return dict(track_to_primary_genre_mapping)

    def _decide_single_track_primary_genre(self, row: Row) -> Tuple[str, Optional[str]]:
        raw_genres = self._convert_row_to_genres(row)

        if raw_genres:
            primary_genre = self._map_raw_genres_to_primary(raw_genres)
        else:
            primary_genre = None

        return row.id, primary_genre

    @staticmethod
    def _convert_row_to_genres(row: Row) -> List[str]:
        genres = []

        if row.spotify_genres is not None:
            genres.extend([genre.lower().strip() for genre in row.spotify_genres])

        if row.shazam_genre is not None:
            genres.append(row.shazam_genre.lower().strip())

        return genres

    def _map_raw_genres_to_primary(self, raw_genres: List[str]) -> PrimaryGenre:
        mapped_genres = [self._genre_mapper.analyze(genre) for genre in raw_genres]
        non_other_main_genres = [genre for genre in mapped_genres if genre != PrimaryGenre.OTHER]

        if non_other_main_genres:
            return self._extract_most_common_main_genre(non_other_main_genres)

        return PrimaryGenre.OTHER

    @staticmethod
    def _extract_most_common_main_genre(main_genres: List[PrimaryGenre]) -> PrimaryGenre:
        main_genres_count = Counter(main_genres)
        most_common_main_genre = main_genres_count.most_common(1)
        genre_name, genre_count = most_common_main_genre[0]

        return genre_name
