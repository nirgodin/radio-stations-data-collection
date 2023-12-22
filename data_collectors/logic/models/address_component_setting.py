from dataclasses import dataclass
from typing import Literal

from genie_datastores.postgres.models import Artist


@dataclass
class AddressComponentSetting:
    column: Artist
    type: Literal["locality", "administrative_area_level_2", "administrative_area_level_1", "country"]
    extract_field: Literal["short_name", "long_name"]
