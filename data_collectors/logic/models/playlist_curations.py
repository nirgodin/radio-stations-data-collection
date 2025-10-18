from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any

from data_collectors.logic.models.curation import Curation


@dataclass
class PlaylistCurations:
    curations: List[Curation]
    tracks: List[Dict[str, Any]]

    @classmethod
    def empty(cls) -> PlaylistCurations:
        return cls(curations=[], tracks=[])
