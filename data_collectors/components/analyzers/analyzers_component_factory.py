from data_collectors.logic.analyzers import *


class AnalyzersComponentFactory:
    @staticmethod
    def get_wikipedia_age_analyzer() -> WikipediaAgeAnalyzer:
        return WikipediaAgeAnalyzer()

    @staticmethod
    def get_primary_genre_analyzer() -> PrimaryGenreAnalyzer:
        return PrimaryGenreAnalyzer()

    @staticmethod
    def get_genre_mapper_analyzer() -> GenreMapperAnalyzer:
        return GenreMapperAnalyzer()
