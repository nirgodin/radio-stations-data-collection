from dataclasses import dataclass
from typing import Dict, Any, Union, Type

from genie_datastores.postgres.models import BaseORMModel


@dataclass
class DBUpdateRequest:
    id: Union[str, int]
    values: Dict[Type[BaseORMModel], Any]
