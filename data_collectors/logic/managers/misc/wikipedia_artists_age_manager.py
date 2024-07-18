from typing import Optional

from data_collectors.contract import IManager
from data_collectors.logic.analyzers import WikipediaAgeAnalyzer
from data_collectors.logic.collectors import BaseWikipediaAgeCollector
from data_collectors.logic.updaters import ValuesDatabaseUpdater


class WikipediaArtistsAgeManager(IManager):
    def __init__(self,
                 age_collector: BaseWikipediaAgeCollector,
                 age_analyzer: WikipediaAgeAnalyzer,
                 db_updater: ValuesDatabaseUpdater):
        self._age_collector = age_collector
        self._age_analyzer = age_analyzer
        self._db_updater = db_updater

    async def run(self, limit: Optional[int] = None):
        summaries = await self._age_collector.collect(limit)
        wikipedia_details = self._age_analyzer.analyze(summaries)
        update_requests = [detail.to_update_request() for detail in wikipedia_details]

        await self._db_updater.update(update_requests)
