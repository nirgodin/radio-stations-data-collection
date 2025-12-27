from dataclasses import dataclass

from genie_datastores.models import DataSource


@dataclass
class RawLyrics:
    id: str
    source: DataSource
    lyrics: str
