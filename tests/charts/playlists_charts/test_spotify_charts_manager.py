from typing import Dict, Type

from _pytest.fixtures import fixture
from genie_datastores.postgres.models import Chart

from data_collectors.consts.charts_consts import SPOTIFY_PLAYLIST_CHART_MAP
from data_collectors.jobs.job_builders.charts.spotify_charts_job_builder import (
    SpotifyChartsJobBuilder,
)
from data_collectors.jobs.job_id import JobId
from tests.charts.playlists_charts.base_playlists_charts_test import (
    BasePlaylistsChartsTest,
)


class TestSpotifyChartsManager(BasePlaylistsChartsTest):
    @fixture
    def playlist_chart_map(self) -> Dict[str, Chart]:
        return SPOTIFY_PLAYLIST_CHART_MAP

    @fixture
    async def job_builder(self) -> Type[SpotifyChartsJobBuilder]:
        return SpotifyChartsJobBuilder

    @fixture
    def job_id(self) -> JobId:
        return JobId.SPOTIFY_CHARTS
