from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import IManager
from data_collectors.tools.josie_client import JosieClient


class JosieCurationsManager(IManager):
    def __init__(self, josie_client: JosieClient, db_engine: AsyncEngine):
        self._josie_client = josie_client
        self._db_engine = db_engine

    async def run(self) -> None:
        pass
