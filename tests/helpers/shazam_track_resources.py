from __future__ import annotations
from dataclasses import dataclass

from random import randint


@dataclass
class ShazamTrackResources:
    id: int
    artist_id: int

    @classmethod
    def random(cls) -> ShazamTrackResources:
        return cls(id=randint(1, 50000), artist_id=randint(1, 50000))
