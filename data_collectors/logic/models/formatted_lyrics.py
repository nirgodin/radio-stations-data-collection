from typing import List

from genie_datastores.models import DataSource
from genie_datastores.mongo.models import LyricsSectionType, LyricsSectionDocument
from pydantic import BaseModel


class FormattedLyricsSection(BaseModel):
    type: LyricsSectionType
    lines: List[str]

    def to_document(self, entity_id: str, source: DataSource, number: int) -> LyricsSectionDocument:
        return LyricsSectionDocument(
            entity_id=entity_id,
            number=number,
            lines=self.lines,
            source=source,
            type=self.type,
        )


class FormattedLyrics(BaseModel):
    sections: List[FormattedLyricsSection]

    def to_documents(self, entity_id: str, source: DataSource) -> List[LyricsSectionDocument]:
        return [section.to_document(entity_id, source, i + 1) for i, section in enumerate(self.sections)]
