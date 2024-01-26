from dataclasses import dataclass
from typing import Dict, Any, Union

from genie_datastores.postgres.models import BaseORMModel


@dataclass
class DBUpdateRequest:
    id: Union[str, int]
    values: Dict[BaseORMModel, Any]
