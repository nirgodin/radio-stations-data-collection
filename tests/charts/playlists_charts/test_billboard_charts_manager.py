from typing import Dict, Type

from _pytest.fixtures import fixture
from genie_datastores.postgres.models import Chart

from data_collectors.consts.charts_consts import (
    BILLBOARD_PLAYLIST_CHART_MAP,
)
from data_collectors.jobs.job_builders.billboard_chart_job_builder import (
    BillboardChartJobBuilder,
)
from data_collectors.jobs.job_id import JobId
from tests.charts.playlists_charts.base_playlists_charts_test import (
    BasePlaylistsChartsTest,
)


class TestBillboardChartsManager(BasePlaylistsChartsTest):
    @fixture
    def playlist_chart_map(self) -> Dict[str, Chart]:
        return BILLBOARD_PLAYLIST_CHART_MAP

    @fixture
    async def job_builder(self) -> Type[BillboardChartJobBuilder]:
        return BillboardChartJobBuilder

    @fixture
    def job_id(self) -> JobId:
        return JobId.BILLBOARD_CHARTS
