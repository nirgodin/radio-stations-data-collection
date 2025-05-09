import asyncio
import os
from datetime import datetime

from genie_datastores.postgres.models import Chart
from spotipyio import SpotifySession
from spotipyio.auth import ClientCredentials, SpotifyGrantType
from spotipyio.tools.auth import AccessCodeFetcher

from data_collectors.charts_creation.charts_creation_utils import get_resource_path
from data_collectors.charts_creation.charts_creator import ChartsCreator
from data_collectors.charts_creation.covers.cover_creator import CoverCreator
from data_collectors.components import ComponentFactory


async def main():
    component_factory = ComponentFactory()
    access_code = AccessCodeFetcher().fetch(["playlist-modify-public", "playlist-modify-private", "ugc-image-upload"])
    credentials = ClientCredentials(grant_type=SpotifyGrantType.AUTHORIZATION_CODE, access_code=access_code)

    async with SpotifySession(credentials=credentials) as spotify_session:
        spotify_client = component_factory.tools.get_spotify_client(spotify_session)
        cover_source_path = get_resource_path("glz_template.jpg")
        charts_creator = ChartsCreator(
            spotify_client=spotify_client,
            db_engine=component_factory.tools.get_database_engine(),
            user_id=os.environ["SPOTIPY_USER_ID"],
            cover_creator=CoverCreator(cover_source_path)
        )

        await charts_creator.create(date=datetime(2013, 1, 3), chart=Chart.GLGLZ_WEEKLY_ISRAELI)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
