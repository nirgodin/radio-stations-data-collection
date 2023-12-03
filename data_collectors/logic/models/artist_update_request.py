from dataclasses import dataclass
from typing import Dict, Any

from genie_datastores.postgres.models import BaseORMModel


@dataclass
class ArtistUpdateRequest:
    artist_id: str
    values: Dict[BaseORMModel, Any]
