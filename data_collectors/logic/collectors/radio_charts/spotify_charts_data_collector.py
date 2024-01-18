from datetime import datetime, date
from typing import List, Dict

from genie_common.tools import logger
from genie_common.utils import safe_nested_get
from genie_datastores.postgres.models import ChartEntry, Chart
from spotipyio import SpotifyClient

from data_collectors.consts.spotify_consts import ITEMS, TRACKS, ID, TRACK, NAME, ARTISTS
from data_collectors.contract import IChartsDataCollector


class SpotifyChartsDataCollector(IChartsDataCollector):
    def __init__(self, spotify_client: SpotifyClient):
        self._spotify_client = spotify_client

    async def collect(self) -> List[ChartEntry]:
        logger.info("Starting to collect spotify charts playlists")
        playlists_ids = list(self._playlist_id_to_chart_mapping.keys())
        playlists = await self._spotify_client.playlists.info.run(playlists_ids)

        return self._to_charts_entries(playlists)

    def _to_charts_entries(self, playlists: List[dict]) -> List[ChartEntry]:
        logger.info("Converting spotify playlists to chart entries")
        charts_entries = []

        for playlist in playlists:
            playlist_entries = self._convert_single_playlist_to_chart_entries(playlist)
            charts_entries.extend(playlist_entries)

        return charts_entries

    def _convert_single_playlist_to_chart_entries(self, playlist: dict) -> List[ChartEntry]:
        tracks = safe_nested_get(playlist, [TRACKS, ITEMS])
        playlist_id = playlist[ID]
        chart = self._playlist_id_to_chart_mapping[playlist_id]
        chart_date = datetime.now().date()

        return self._create_chart_entries(
            tracks=tracks,
            playlist_id=playlist_id,
            chart=chart,
            chart_date=chart_date
        )

    @staticmethod
    def _create_chart_entries(tracks: List[dict], playlist_id: str, chart: Chart, chart_date: date) -> List[ChartEntry]:
        chart_entries = []

        for i, track in enumerate(tracks):
            track_entry = ChartEntry(
                track_id=safe_nested_get(track, [TRACK, ID]),
                chart=chart,
                date=chart_date,
                position=i + 1,
                comment=playlist_id
            )
            chart_entries.append(track_entry)

        return chart_entries

    @property
    def _playlist_id_to_chart_mapping(self) -> Dict[str, Chart]:
        return {
            '37i9dQZEVXbJ6IpvItkve3': Chart.SPOTIFY_DAILY_ISRAELI,
            '37i9dQZEVXbMDoHDwVN2tF': Chart.SPOTIFY_DAILY_INTERNATIONAL,
        }
