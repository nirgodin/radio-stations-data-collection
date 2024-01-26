import os

from genie_common.tools import logger
from genie_datastores.google.sheets import GoogleSheetsClient

from data_collectors import ChartsTaggedMistakesCollector
from data_collectors.contract import IManager


class ChartsTaggedMistakesManager(IManager):
    def __init__(self, sheets_client: GoogleSheetsClient, tagged_mistakes_collector: ChartsTaggedMistakesCollector):
        self._sheets_client = sheets_client
        self._tagged_mistakes_collector = tagged_mistakes_collector

    async def run(self):
        logger.info("Reading tagged mistakes data from sheet")
        data = self._sheets_client.read(
            spreadsheet=os.environ["MISTAKES_DATA_SPREADSHEET_URL"],
            worksheet_name=os.environ["MISTAKES_DATA_WORKSHEET_NAME"]
        )
        update_requests = self._tagged_mistakes_collector.collect(data)
        # TODO: Generalize updater and add here
        # TODO: Allow nulls on track id column
        logger.info("Setting all spreadsheet records as done and overwriting existing data")
        data["done"] = True
        self._sheets_client.write(
            spreadsheet=os.environ["MISTAKES_DATA_SPREADSHEET_URL"],
            worksheet_name=os.environ["MISTAKES_DATA_WORKSHEET_NAME"],
            data=data,
            overwrite=True
        )
