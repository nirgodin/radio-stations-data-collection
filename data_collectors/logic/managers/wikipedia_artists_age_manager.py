from typing import Optional

from data_collectors.contract import IManager
from data_collectors.logic.collectors import BaseWikipediaAgeCollector
from data_collectors.logic.updaters import ValuesDatabaseUpdater


class WikipediaArtistsAgeManager(IManager):
    def __init__(self, age_collector: BaseWikipediaAgeCollector, db_updater: ValuesDatabaseUpdater):
        self._age_collector = age_collector
        self._db_updater = db_updater

    async def run(self, limit: Optional[int] = None):
        wikipedia_details = await self._age_collector.collect(limit)
        update_requests = [detail.to_update_request() for detail in wikipedia_details]

        await self._db_updater.update(update_requests)
