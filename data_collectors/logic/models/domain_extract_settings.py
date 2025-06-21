from dataclasses import dataclass
from typing import Callable, Type
from urllib.parse import ParseResult

from genie_datastores.postgres.models import SpotifyArtist

from data_collectors.logic.models.domain import Domain


@dataclass
class DomainExtractSettings:
    domain: Domain
    extract_fn: Callable[[ParseResult], str]
    column: Type[SpotifyArtist]
