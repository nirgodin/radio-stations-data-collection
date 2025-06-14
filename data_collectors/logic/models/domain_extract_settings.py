from dataclasses import dataclass
from typing import Callable, Type
from urllib.parse import ParseResult

from genie_datastores.postgres.models import SpotifyArtist


@dataclass
class DomainExtractSettings:
    domain: str
    extract_fn: Callable[[ParseResult], str]
    column: Type[SpotifyArtist]
