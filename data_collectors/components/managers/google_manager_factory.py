from aiohttp import ClientSession

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.collectors import BaseArtistsExistingDetailsCollector
from data_collectors.logic.managers import (
    GoogleArtistsOriginGeocodingManager,
    GoogleArtistsWebPagesManager,
    GeminiArtistsAboutManager,
)


class GoogleManagerFactory(BaseManagerFactory):
    def get_artists_origin_geocoding_manager(self, session: ClientSession) -> GoogleArtistsOriginGeocodingManager:
        geocoding_collector = self.collectors.google.get_geocoding_collector(session)

        return GoogleArtistsOriginGeocodingManager(
            geocoding_collector=geocoding_collector,
            db_updater=self.updaters.get_values_updater(),
        )

    async def get_spotify_artists_about_manager(self) -> GeminiArtistsAboutManager:
        existing_details_collector = await self.collectors.spotify.get_artist_existing_details_collector()
        return self._get_artists_about_manager(existing_details_collector)

    async def get_shazam_artists_about_manager(self) -> GeminiArtistsAboutManager:
        existing_details_collector = await self.collectors.shazam.get_artist_existing_details_collector()
        return self._get_artists_about_manager(existing_details_collector)

    def get_wikipedia_artists_about_manager(self) -> GeminiArtistsAboutManager:
        return self._get_artists_about_manager(self.collectors.wikipedia.get_wikipedia_existing_details_collector())

    async def get_genius_artists_about_manager(self) -> GeminiArtistsAboutManager:
        existing_details_collector = await self.collectors.genius.get_artists_existing_details_collector()
        return self._get_artists_about_manager(existing_details_collector)

    def get_artists_web_pages_manager(self, session: ClientSession) -> GoogleArtistsWebPagesManager:
        return GoogleArtistsWebPagesManager(
            db_engine=self.tools.get_database_engine(),
            web_pages_collector=self.collectors.google.get_artists_web_pages_collector(session),
            web_pages_serializer=self.serializers.get_artists_web_pages_serializer(),
            db_updater=self.updaters.get_values_updater(),
        )

    def _get_artists_about_manager(
        self, existing_details_collector: BaseArtistsExistingDetailsCollector
    ) -> GeminiArtistsAboutManager:
        return GeminiArtistsAboutManager(
            existing_details_collector=existing_details_collector,
            parsing_collector=self.collectors.google.get_artists_about_parsing_collector(),
            pool_executor=self.tools.get_pool_executor(),
            db_engine=self.tools.get_database_engine(),
            db_updater=self.updaters.get_values_updater(),
        )
