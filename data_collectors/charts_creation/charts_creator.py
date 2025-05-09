from datetime import datetime
from typing import List, Dict

from genie_common.tools import logger
from genie_datastores.postgres.models import Chart, ChartEntry
from genie_datastores.postgres.operations import execute_query
from spotipyio import SpotifyClient
from spotipyio.logic.utils import to_uri
from spotipyio.models import PlaylistCreationRequest, EntityType
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.charts_creation.charts_creation_consts import WEEKLY_CHART_IMAGE_TEXT, FONT_PATH, DATE_FONT_SIZE, \
    DATE_POSITION, DATE_COLOR, ISRAELI_IMAGE_TEXT, INTERNATIONAL_IMAGE_TEXT, PLAYLIST_NAME_FORMAT
from data_collectors.charts_creation.covers.cover_creator import CoverCreator
from data_collectors.charts_creation.covers.image_text import ImageText, Font
from data_collectors.charts_creation.playlist_description_builder import PlaylistDescriptionBuilder


class ChartsCreator:
    def __init__(
        self,
        spotify_client: SpotifyClient,
        db_engine: AsyncEngine,
        user_id: str,
        cover_creator: CoverCreator,
        description_builder: PlaylistDescriptionBuilder = PlaylistDescriptionBuilder(),
    ):
        self._spotify_client = spotify_client
        self._db_engine = db_engine
        self._user_id = user_id
        self._cover_creator = cover_creator
        self._description_builder = description_builder

    async def create(self, date: datetime, chart: Chart) -> None:
        formatted_date = date.strftime('%Y-%m-%d')
        entries = await self._query_chart_entries(
            date=date,
            chart=chart,
            formatted_date=formatted_date
        )
        playlist_id = await self._create_empty_playlist(
            entries=entries,
            chart=chart,
            formatted_date=formatted_date
        )
        await self._add_playlist_items(entries, playlist_id)
        await self._set_playlist_cover(
            playlist_id=playlist_id,
            chart=chart,
            formatted_date=formatted_date
        )

    async def _query_chart_entries(self, date: datetime, chart: Chart, formatted_date: str) -> List[ChartEntry]:
        logger.info(f"Querying database for charts entries of chart {chart.value} for date {formatted_date}")
        query = select(ChartEntry).where(ChartEntry.chart == chart).where(ChartEntry.date == date)
        query_result = await execute_query(self._db_engine, query)

        return query_result.scalars().all()

    async def _create_empty_playlist(self, entries: List[ChartEntry], chart: Chart, formatted_date: str) -> str:
        logger.info(f"Creating empty playlist of chart {chart.value} for date {formatted_date}")
        name = self._build_playlist_name(chart, formatted_date)
        description = self._description_builder.build(
            entries=entries,
            chart=chart,
            formatted_date=formatted_date,
        )
        request = PlaylistCreationRequest(
            user_id=self._user_id,
            name=name,
            description=description,
            public=True,
        )
        response = await self._spotify_client.playlists.create.run(request)
        playlist_id = response["id"]
        logger.info(f"Successfully created empty playlist with id: {playlist_id}")

        return playlist_id

    def _build_playlist_name(self, chart: Chart, formatted_date: str) -> str:
        chart_type = self._chart_name_map[chart]
        return PLAYLIST_NAME_FORMAT.format(chart_type=chart_type, chart_date=formatted_date)

    async def _add_playlist_items(self, entries: List[ChartEntry], playlist_id: str) -> None:
        existing_entries = [entry for entry in entries if entry.track_id is not None]
        if not existing_entries:
            logger.warn(f"Did not find any valid track id to add to playlist with ID: {playlist_id}. Skipping.")
            return

        logger.info(f"Adding {len(existing_entries)} tracks to playlist with ID: {playlist_id}")
        uris = [to_uri(entity_id=entry.track_id, entity_type=EntityType.TRACK) for entry in existing_entries]

        await self._spotify_client.playlists.add_items.run(
            playlist_id=playlist_id,
            uris=uris
        )

    async def _set_playlist_cover(self, playlist_id: str, chart: Chart, formatted_date: str) -> None:
        logger.info(f"Setting cover for playlist with id: {playlist_id}")
        image_texts = self._build_image_texts(chart, formatted_date)
        image = self._cover_creator.create(image_texts)

        await self._spotify_client.playlists.update_cover.run(
            playlist_id=playlist_id,
            image=image,
            compress_if_needed=True,
        )

    def _build_image_texts(self, chart: Chart, formatted_date: str) -> List[ImageText]:
        chart_text = self._chart_image_text_map[chart]
        date_image_text = ImageText(
            font=Font(path=FONT_PATH, size=DATE_FONT_SIZE),
            text=formatted_date,
            position=DATE_POSITION,
            color=DATE_COLOR,
        )

        return [
            WEEKLY_CHART_IMAGE_TEXT,
            chart_text,
            date_image_text
        ]

    @property
    def _chart_image_text_map(self) -> Dict[Chart, ImageText]:
        return {
            Chart.GLGLZ_WEEKLY_ISRAELI: ISRAELI_IMAGE_TEXT,
            Chart.GLGLZ_WEEKLY_INTERNATIONAL: INTERNATIONAL_IMAGE_TEXT,
        }

    @property
    def _chart_name_map(self) -> Dict[Chart, str]:
        return {
            Chart.GLGLZ_WEEKLY_ISRAELI: b'\xd7\x94\xd7\x99\xd7\xa9\xd7\xa8\xd7\x90\xd7\x9c\xd7\x99'.decode('utf-8'),
            Chart.GLGLZ_WEEKLY_INTERNATIONAL: b'\xd7\x94\xd7\x91\xd7\x99\xd7\xa0\xd7\x9c\xd7\x90\xd7\x95\xd7\x9e\xd7\x99'.decode('utf-8'),
        }
