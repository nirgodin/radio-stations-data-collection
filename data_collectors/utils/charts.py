from typing import Tuple


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
