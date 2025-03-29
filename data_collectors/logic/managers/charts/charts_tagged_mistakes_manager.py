import os
from typing import List

from genie_common.tools import logger
from genie_datastores.google.sheets import GoogleSheetsClient
from pandas import DataFrame

from data_collectors.logic.collectors import (
    ChartsTaggedMistakesCollector,
    ChartsTaggedMistakesTracksCollector,
)
from data_collectors.logic.inserters.postgres import SpotifyInsertionsManager
from data_collectors.logic.models import DBUpdateRequest
from data_collectors.logic.updaters import ValuesDatabaseUpdater
from data_collectors.contract import IManager


class ChartsTaggedMistakesManager(IManager):
    def __init__(
        self,
        sheets_client: GoogleSheetsClient,
        tagged_mistakes_collector: ChartsTaggedMistakesCollector,
        tagged_mistakes_tracks_collector: ChartsTaggedMistakesTracksCollector,
        spotify_insertions_manager: SpotifyInsertionsManager,
        db_updater: ValuesDatabaseUpdater,
    ):
        self._sheets_client = sheets_client
        self._tagged_mistakes_collector = tagged_mistakes_collector
        self._tagged_mistakes_tracks_collector = tagged_mistakes_tracks_collector
        self._spotify_insertions_manager = spotify_insertions_manager
        self._db_updater = db_updater

    async def run(self):
        logger.info("Reading tagged mistakes data from sheet")
        data = self._sheets_client.read(
            spreadsheet=os.environ["MISTAKES_DATA_SPREADSHEET_URL"],
            worksheet_name=os.environ["MISTAKES_DATA_WORKSHEET_NAME"],
        )
        update_requests = await self._tagged_mistakes_collector.collect(data)
        tracks = await self._tagged_mistakes_tracks_collector.collect(update_requests)
        await self._insert_records(tracks, update_requests)
        self._update_mistakes_sheet(data)

    async def _insert_records(self, tracks: List[dict], update_requests: List[DBUpdateRequest]) -> None:
        if tracks:
            logger.info("Found non existing spotify tracks. Inserting to spotify tables before updating charts entries")
            await self._spotify_insertions_manager.insert(tracks)

        await self._db_updater.update(update_requests)

    def _update_mistakes_sheet(self, data: DataFrame) -> None:
        logger.info("Setting all spreadsheet records as done and overwriting existing data")
        data["done"] = True

        self._sheets_client.write(
            spreadsheet=os.environ["MISTAKES_DATA_SPREADSHEET_URL"],
            worksheet_name=os.environ["MISTAKES_DATA_WORKSHEET_NAME"],
            data=data,
            overwrite=True,
        )
