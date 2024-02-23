from asyncio import sleep
from typing import Optional

from genie_common.tools import logger
from playwright.async_api import Page


async def get_page_content(page: Page) -> Optional[str]:
    n_retries = 0

    while n_retries < 3:
        content = await page.content()

        if content is not None:
            return content

        n_retries += 1
        await sleep(1)

    logger.warn("Could not get page content. Skipping")
