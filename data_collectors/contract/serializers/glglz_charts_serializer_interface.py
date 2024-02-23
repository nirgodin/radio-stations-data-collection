from abc import ABC
from typing import Any, List, Dict

from genie_datastores.postgres.models import ChartEntry

from data_collectors.contract.serializer_interface import ISerializer
from data_collectors.logic.models import GlglzChartDetails


class IGlglgzChartsSerializer(ISerializer, ABC):
    def serialize(self, chart_details: GlglzChartDetails, elements: List[Dict[str, str]]) -> List[ChartEntry]:
        raise NotImplementedError
