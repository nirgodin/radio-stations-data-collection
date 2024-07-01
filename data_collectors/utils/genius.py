from genie_common.utils import safe_nested_get

from data_collectors.consts.genius_consts import META, STATUS


def is_valid_response(response: dict) -> bool:
    status_code = safe_nested_get(response, [META, STATUS])
    return status_code == 200
