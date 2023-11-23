from typing import Any, Optional


def merge_dicts(*dicts: Optional[dict]) -> dict:
    merged_dict = {}

    for d in dicts:
        if isinstance(d, dict):
            merged_dict.update(d)

    return merged_dict


def safe_nested_get(dct: dict, paths: list, default: Optional[Any] = None) -> Any:
    value = dct.get(paths[0], {})

    for path in paths[1:]:
        value = value.get(path, {})

    return value if value != {} else default
