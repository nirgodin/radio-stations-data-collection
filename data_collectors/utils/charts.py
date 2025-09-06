from typing import Tuple

from genie_common.tools import logger

from data_collectors.consts.glglz_consts import UNAVAILABLE_GLGLZ_CHART_SUBSTRINGS


def extract_artist_and_track_from_chart_key(key: str) -> Tuple[str, str]:
    key_components = key.split("-")
    if len(key_components) == 1:
        key_components = key.split("–")

    n_component = len(key_components)

    if n_component < 2:
        return key, ""
    if n_component == 2:
        return key_components[0].strip(), key_components[1].strip()

    return key_components[0].strip(), "-".join(key_components[1:]).strip()


def is_valid_glglz_chart_url(page_source: str, url: str) -> bool:
    if any(substring.lower() in page_source.lower() for substring in UNAVAILABLE_GLGLZ_CHART_SUBSTRINGS):
        logger.info(f"Did not manage to find charts entries in url `{url}`. Skipping")
        return False

    logger.info(f"Found charts entries in url `{url}`! Parsing page source")
    return True
