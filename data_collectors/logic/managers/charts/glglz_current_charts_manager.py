from typing import Dict, Any

from data_collectors.logic.managers.charts.base_charts_manager import BaseChartsManager


class GlglzCurrentChartsManager(BaseChartsManager):
    async def _generate_data_collector_kwargs(self) -> Dict[str, Any]:
        return {}
