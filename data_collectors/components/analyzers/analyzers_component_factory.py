from data_collectors.logic.analyzers.wikipedia_age_analyzer import WikipediaAgeAnalyzer


class AnalyzersComponentFactory:
    @staticmethod
    def get_wikipedia_age_analyzer() -> WikipediaAgeAnalyzer:
        return WikipediaAgeAnalyzer()
