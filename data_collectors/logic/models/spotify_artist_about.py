from dataclasses import dataclass
from typing import Optional

from genie_datastores.models import EntityType, DataSource
from genie_datastores.mongo.models import AboutDocument
from genie_datastores.postgres.models import SpotifyArtist

from data_collectors.logic.models.database_update_request import DBUpdateRequest


@dataclass
class SpotifyArtistAbout:
    id: str
    name: str
    facebook_name: Optional[str] = None
    instagram_name: Optional[str] = None
    twitter_name: Optional[str] = None
    about: Optional[str] = None

    def to_update_request(self) -> DBUpdateRequest:
        return DBUpdateRequest(
            id=self.id,
            values={
                SpotifyArtist.facebook_name: self.facebook_name,
                SpotifyArtist.instagram_name: self.instagram_name,
                SpotifyArtist.twitter_name: self.twitter_name,
            }
        )

    def to_about_document(self) -> AboutDocument:
        return AboutDocument(
            about=self.about,
            entity_type=EntityType.ARTIST,
            entity_id=self.id,
            name=self.name,
            source=DataSource.SPOTIFY
        )
