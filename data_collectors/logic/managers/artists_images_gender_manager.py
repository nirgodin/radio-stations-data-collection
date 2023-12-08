from typing import List, Optional, Dict

from genie_datastores.postgres.models import SpotifyArtist, Gender, DataSource
from genie_datastores.postgres.operations import execute_query
from numpy import ndarray
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.logic.collectors import SpotifyArtistsImagesCollector
from data_collectors.contract import IManager
from data_collectors.logic.models import DBUpdateRequest
from data_collectors.logic.updaters.spotify_artists_database_updater import SpotifyArtistsDatabaseUpdater
from data_collectors.tools import ImageGenderDetector


class ArtistsImagesGenderManager(IManager):
    def __init__(self,
                 db_engine: AsyncEngine,
                 artists_images_collector: SpotifyArtistsImagesCollector,
                 gender_detector: ImageGenderDetector,
                 gender_updater: SpotifyArtistsDatabaseUpdater):
        self._db_engine = db_engine
        self._artists_images_collector = artists_images_collector
        self._gender_detector = gender_detector
        self._gender_updater = gender_updater

    async def run(self, limit: Optional[int]) -> None:
        artists_ids = await self._query_missing_gender_artists(limit)
        ids_images_mapping = await self._artists_images_collector.collect(artists_ids)
        update_requests = self._build_update_requests(ids_images_mapping)

        await self._gender_updater.update(update_requests)

    async def _query_missing_gender_artists(self, limit: Optional[int]) -> List[str]:
        query = (
            select(SpotifyArtist.id)
            .where(SpotifyArtist.gender.is_(None))
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().all()

    def _build_update_requests(self, ids_images_mapping: Dict[str, ndarray]) -> List[DBUpdateRequest]:
        update_requests = []

        for artist_id, image in ids_images_mapping.items():
            request = DBUpdateRequest(
                id=artist_id,
                values={
                    SpotifyArtist.gender: self._determine_artist_gender(image),
                    SpotifyArtist.gender_source: DataSource.SPOTIFY_IMAGES
                }
            )
            update_requests.append(request)

        return update_requests

    def _determine_artist_gender(self, image: ndarray) -> Optional[Gender]:
        detected_genders = self._gender_detector.detect_gender(image)
        n_genders = len(detected_genders)

        if n_genders == 0:
            return None

        if n_genders == 1:
            return detected_genders[0].gender

        return Gender.BAND
