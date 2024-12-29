from typing import Type

from genie_datastores.postgres.models import Artist, SpotifyTrack, TrackLyrics
from genie_datastores.models import DataSource, EntityType
from sqlalchemy import select
from sqlalchemy.sql import Select

from data_collectors.logic.managers.translations.base_translation_manager import (
    BaseTranslationManager,
)


class SpotifyIsraeliTracksTranslationManager(BaseTranslationManager):
    @property
    def _query(self) -> Select:
        return (
            select(SpotifyTrack.id, SpotifyTrack.name)
            .where(SpotifyTrack.id == TrackLyrics.id)
            .where(SpotifyTrack.artist_id == Artist.id)
            .where(TrackLyrics.language == "Hebrew")
            .where(Artist.is_israeli.is_(True))
            .where(SpotifyTrack.name.regexp_match(r"^[a-zA-Z0-9\s]+$"))
        )

    @property
    def _orm(self) -> Type[SpotifyTrack]:
        return SpotifyTrack

    @property
    def _entity_source(self) -> DataSource:
        return DataSource.SPOTIFY

    @property
    def _entity_type(self) -> EntityType:
        return EntityType.TRACK
