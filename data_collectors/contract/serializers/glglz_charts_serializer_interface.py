from abc import ABC, abstractmethod
from typing import List, Dict

from genie_datastores.postgres.models import ChartEntry

from data_collectors.contract.serializer_interface import ISerializer
from data_collectors.logic.models import GlglzChartDetails, HTMLElement


class IGlglzChartsSerializer(ISerializer, ABC):
    @abstractmethod
    def serialize(
        self, chart_details: GlglzChartDetails, elements: List[Dict[str, str]]
    ) -> List[ChartEntry]:
        raise NotImplementedError

    @property
    @abstractmethod
    def element(self) -> HTMLElement:
        raise NotImplementedError
