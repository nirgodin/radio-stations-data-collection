from enum import Enum
from typing import Type, List


def get_all_enum_values(enum_: Type[Enum]) -> List[Enum]:
    return [v for v in enum_]
