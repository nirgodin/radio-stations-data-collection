from typing import Type

from genie_datastores.postgres.models import SpotifyArtist, Artist
from genie_datastores.models import DataSource, EntityType
from sqlalchemy import select
from sqlalchemy.sql import Select

from data_collectors.logic.managers.translations.base_translation_manager import BaseTranslationManager


class SpotifyIsraeliArtistsTranslationManager(BaseTranslationManager):
    @property
    def _query(self) -> Select:
        return (
            select(SpotifyArtist.id, SpotifyArtist.name)
            .where(SpotifyArtist.id == Artist.id)
            .where(Artist.is_israeli.is_(True))
            .where(SpotifyArtist.name.regexp_match(r'^[a-zA-Z0-9\s]+$'))
        )

    @property
    def _orm(self) -> Type[SpotifyArtist]:
        return SpotifyArtist

    @property
    def _entity_source(self) -> DataSource:
        return DataSource.SPOTIFY

    @property
    def _entity_type(self) -> EntityType:
        return EntityType.ARTIST
