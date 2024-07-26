from dataclasses import dataclass
from typing import Optional

from genie_datastores.models import EntityType, DataSource
from genie_datastores.mongo.models import AboutDocument
from sqlalchemy.engine import Row


@dataclass
class WikipediaArtistAbout:
    id: str
    name: str
    wikipedia_name: str
    wikipedia_language: str
    about: Optional[str] = None

    @classmethod
    def from_row(cls, row: Row) -> "WikipediaArtistAbout":
        return cls(
            id=row.id,
            name=row.name,
            wikipedia_name=row.wikipedia_name,
            wikipedia_language=row.wikipedia_language
        )

    def to_about_document(self) -> AboutDocument:
        return AboutDocument(
            about=self.about,
            entity_type=EntityType.ARTIST,
            entity_id=self.id,
            name=self.name,
            source=DataSource.GENERAL_WIKIPEDIA  # TODO: Add Wikipedia as separate source
        )
