from dataclasses import dataclass

from genie_datastores.postgres.models import Gender


@dataclass
class ArtistGender:
    gender: Gender
    confidence: float
