from typing import Any, Dict

from data_collectors.logic.managers.radio_charts.base_radio_charts_manager import BaseRadioChartsManager


class SpotifyChartsManager(BaseRadioChartsManager):
    async def _generate_data_collector_order_args(self) -> Dict[str, Any]:
        return {}
