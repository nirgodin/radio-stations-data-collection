from typing import Type

from genie_datastores.postgres.models import (
    Artist,
    ShazamTrack,
    TrackIDMapping,
    SpotifyTrack,
    TrackLyrics,
)
from genie_datastores.models import DataSource, EntityType
from sqlalchemy import select
from sqlalchemy.sql import Select

from data_collectors.logic.managers.translations.base_translation_manager import (
    BaseTranslationManager,
)


class ShazamIsraeliTracksTranslationManager(BaseTranslationManager):
    @property
    def _query(self) -> Select:
        return (
            select(ShazamTrack.id, ShazamTrack.name)
            .where(ShazamTrack.id == TrackIDMapping.shazam_id)
            .where(TrackIDMapping.id == SpotifyTrack.id)
            .where(SpotifyTrack.id == TrackLyrics.id)
            .where(TrackLyrics.language == "Hebrew")
            .where(SpotifyTrack.artist_id == Artist.id)
            .where(Artist.is_israeli.is_(True))
            .where(ShazamTrack.name.regexp_match(r"^[a-zA-Z0-9\s]+$"))
        )

    @property
    def _orm(self) -> Type[ShazamTrack]:
        return ShazamTrack

    @property
    def _entity_source(self) -> DataSource:
        return DataSource.SHAZAM

    @property
    def _entity_type(self) -> EntityType:
        return EntityType.TRACK
