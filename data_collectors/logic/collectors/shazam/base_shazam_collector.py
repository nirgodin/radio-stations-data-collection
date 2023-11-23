from abc import ABC

from shazamio import Shazam

from data_collectors.contract.collector_interface import ICollector
from data_collectors.tools import AioPoolExecutor


class BaseShazamCollector(ICollector, ABC):
    def __init__(self, shazam: Shazam, pool_executor: AioPoolExecutor):
        self._shazam = shazam
        self._pool_executor = pool_executor
