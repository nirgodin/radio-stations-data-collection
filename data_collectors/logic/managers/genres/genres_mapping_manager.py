from typing import List

from genie_common.tools import logger
from genie_datastores.google.sheets import GoogleSheetsClient
from genie_datastores.postgres.models import Genre, PrimaryGenre
from pandas import DataFrame

from data_collectors.contract import IManager
from data_collectors.logic.inserters.postgres import GenresDatabaseInserter

GENRE_COLUMN = "genre"


class GenresMappingManager(IManager):
    def __init__(
        self, sheets_client: GoogleSheetsClient, genres_inserter: GenresDatabaseInserter
    ):
        self._sheets_client = sheets_client
        self._genres_inserter = genres_inserter

    async def run(self, spreadsheet_url: str, worksheet_name: str) -> None:
        logger.info("Starting to read genres mapping from spreadsheet")
        data = self._sheets_client.read(
            spreadsheet=spreadsheet_url, worksheet_name=worksheet_name
        )
        data.drop_duplicates(subset=[GENRE_COLUMN], inplace=True)
        data = data[data[GENRE_COLUMN] != ""]
        records = self._to_records(data)

        await self._genres_inserter.insert(records)

    @staticmethod
    def _to_records(data: DataFrame) -> List[Genre]:
        logger.info(f"Transforming {len(data)} rows to Genre records")
        records = []

        for _, row in data.iterrows():
            primary_genre = row["primary_genre"].lower().strip()
            record = Genre(
                id=row[GENRE_COLUMN].lower().strip(),
                primary_genre=PrimaryGenre(primary_genre),
            )
            records.append(record)

        return records
