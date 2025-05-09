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
from data_collectors.charts_creation.multi_charts_creator import MultiChartsCreator
from data_collectors.components import ComponentFactory


async def main():
    access_code = AccessCodeFetcher().fetch(["playlist-modify-public", "playlist-modify-private", "ugc-image-upload"])
    credentials = ClientCredentials(grant_type=SpotifyGrantType.AUTHORIZATION_CODE, access_code=access_code)

    async with SpotifySession(credentials=credentials) as spotify_session:
        multi_charts_creator = _build_charts_creator(spotify_session)
        await multi_charts_creator.create(limit=None)


def _build_charts_creator(spotify_session: SpotifySession) -> MultiChartsCreator:
    component_factory = ComponentFactory()
    spotify_client = component_factory.tools.get_spotify_client(spotify_session)
    cover_source_path = get_resource_path("glz_template.jpg")
    db_engine = component_factory.tools.get_database_engine()
    charts_creator = ChartsCreator(
        spotify_client=spotify_client,
        db_engine=db_engine,
        user_id=os.environ["SPOTIPY_USER_ID"],
        cover_creator=CoverCreator(cover_source_path),
    )

    return MultiChartsCreator(
        db_engine=db_engine, pool_executor=component_factory.tools.get_pool_executor(), charts_creator=charts_creator
    )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
