from functools import lru_cache, partial

from genie_common.utils import run_async
from wikipediaapi import Wikipedia

from data_collectors.contract import ICollector


class WikipediaPageSummaryCollector(ICollector):
    async def collect(self, name: str, language: str) -> str:
        wikipedia = self._get_wikipedia(language)
        func = partial(wikipedia.page, name)
        page = await run_async(func)

        return page.summary

    @lru_cache
    def _get_wikipedia(self, language: str) -> Wikipedia:
        return Wikipedia(language)
