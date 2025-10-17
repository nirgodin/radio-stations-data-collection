from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from genie_datastores.models import DataSource
from genie_datastores.postgres.models import Curator, CuratorCollection, CuratedTrack


@dataclass
class Curation:
    collection_id: str
    curator_id: str
    curator_name: str
    date: datetime
    description: Optional[str]
    source: DataSource
    title: Optional[str]
    track_id: str
    collection_comment: Optional[str] = None

    def to_curator(self) -> Curator:
        return Curator(id=self.curator_id, name=self.curator_name, source=self.source)

    def to_collection(self) -> CuratorCollection:
        return CuratorCollection(
            id=self.collection_id,
            curator_id=self.curator_id,
            title=self.title,
            description=self.description,
        )

    def to_track(self) -> CuratedTrack:
        return CuratedTrack(
            track_id=self.track_id,
            date=self.date,
            collection=self.collection_id,
        )
