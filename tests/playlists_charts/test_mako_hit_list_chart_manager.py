from typing import Dict

from _pytest.fixtures import fixture
from genie_datastores.postgres.models import Chart

from data_collectors.components import ComponentFactory
from data_collectors.consts.charts_consts import MAKO_PLAYLIST_CHART_MAP
from data_collectors.jobs.job_builders.mako_hit_list_chart_job_builder import (
    MakoHitListChartJobBuilder,
)
from tests.playlists_charts.base_playlists_charts_test import BasePlaylistsChartsTest


class TestMakoHitListChartsManager(BasePlaylistsChartsTest):
    @fixture
    def playlist_chart_map(self) -> Dict[str, Chart]:
        return MAKO_PLAYLIST_CHART_MAP

    @fixture
    async def job_builder(
        self, component_factory: ComponentFactory
    ) -> MakoHitListChartJobBuilder:
        return MakoHitListChartJobBuilder(component_factory)
