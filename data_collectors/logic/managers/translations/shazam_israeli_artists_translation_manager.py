from typing import Type

from genie_datastores.postgres.models import Artist, ShazamArtist
from genie_datastores.models import DataSource, EntityType
from sqlalchemy import select
from sqlalchemy.sql import Select

from data_collectors.logic.managers.translations.base_translation_manager import BaseTranslationManager


class ShazamIsraeliArtistsTranslationManager(BaseTranslationManager):
    @property
    def _query(self) -> Select:
        return (
            select(ShazamArtist.id, ShazamArtist.name)
            .where(ShazamArtist.id == Artist.shazam_id)
            .where(Artist.is_israeli.is_(True))
            .where(ShazamArtist.name.regexp_match(r'^[a-zA-Z0-9\s]+$'))
        )

    @property
    def _orm(self) -> Type[ShazamArtist]:
        return ShazamArtist

    @property
    def _entity_source(self) -> DataSource:
        return DataSource.SHAZAM

    @property
    def _entity_type(self) -> EntityType:
        return EntityType.ARTIST
