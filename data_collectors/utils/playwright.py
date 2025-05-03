from asyncio import sleep
from typing import Optional

from genie_common.tools import logger
from playwright.async_api import Page


async def get_page_content(page: Page, max_retries: int = 3, sleep_between: int = 1) -> Optional[str]:
    n_retries = 0

    while n_retries < max_retries:
        content = await page.content()

        if content is not None:
            return content

        n_retries += 1
        await sleep(sleep_between)

    logger.warn("Could not get page content. Skipping")
