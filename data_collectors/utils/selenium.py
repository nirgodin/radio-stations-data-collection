from contextlib import contextmanager
from typing import Generator

from selenium.webdriver import Chrome, ChromeOptions


@contextmanager
def driver_session() -> Generator[Chrome, None, None]:
    driver = None

    try:
        options = ChromeOptions()
        options.add_argument("--headless")
        driver = Chrome(options=options)

        yield driver

    finally:
        if driver is not None:
            driver.quit()
