from typing import Any, Dict

from data_collectors.logic.managers.charts.base_charts_manager import BaseChartsManager


class PlaylistsChartsManager(BaseChartsManager):
    async def _generate_data_collector_order_args(self) -> Dict[str, Any]:
        return {}
