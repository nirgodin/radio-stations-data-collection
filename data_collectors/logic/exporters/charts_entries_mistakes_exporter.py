from typing import Any

from genie_common.tools import SyncPoolExecutor, logger
from genie_common.utils import compute_similarity_score
from genie_datastores.google.sheets import GoogleSheetsUploader, Sheet
from genie_datastores.postgres.models import ChartEntry, SpotifyTrack, SpotifyArtist
from genie_datastores.postgres.operations import read_sql
from pandas import DataFrame, Series
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.general_consts import SIMILARITY
from data_collectors.consts.musixmatch_consts import ARTIST_NAME
from data_collectors.consts.shazam_consts import KEY
from data_collectors.consts.spotify_consts import NAME
from data_collectors.contract import IExporter

ACTUAL_KEY = "actual_key"


class ChartsEntriesMistakesExporter(IExporter):
    def __init__(self,
                 db_engine: AsyncEngine,
                 sheets_uploader: GoogleSheetsUploader,
                 pool_executor: SyncPoolExecutor = SyncPoolExecutor()):
        self._db_engine = db_engine
        self._sheets_uploader = sheets_uploader
        self._pool_executor = pool_executor

    async def export(self) -> Any:
        data = await self._query_data()
        self._pre_process_output_data(data)
        logger.info("Uploading resulting data to google sheet")
        spreadsheet = self._sheets_uploader.upload(
            title="Charts Entries Mistakes Candidates",
            sheets=[Sheet(data, "candidates")]
        )
        logger.info(f"Exported data to sheet with url {spreadsheet.url}")

    async def _query_data(self) -> DataFrame:
        logger.info("Querying database for unique charts keys")
        query = (
            select(ChartEntry.key, ChartEntry.chart, SpotifyTrack.name, SpotifyArtist.name.label(ARTIST_NAME))
            .distinct(ChartEntry.key)
            .where(ChartEntry.key.isnot(None))
            .where(ChartEntry.track_id == SpotifyTrack.id)
            .where(SpotifyTrack.artist_id == SpotifyArtist.id)
        )
        data = await read_sql(engine=self._db_engine, query=query)

        return data.fillna("")

    def _pre_process_output_data(self, data: DataFrame) -> None:
        logger.info("Pre processing data")
        data[ACTUAL_KEY] = data[ARTIST_NAME] + " - " + data[NAME]
        similarities = self._pool_executor.run(
            iterable=[row for _, row in data.iterrows()],
            func=self._compute_keys_similarity,
            expected_type=float
        )
        data[SIMILARITY] = similarities
        data.sort_values(by=SIMILARITY, ascending=True, inplace=True)

    @staticmethod
    def _compute_keys_similarity(row: Series) -> float:
        return compute_similarity_score(row[KEY], row[ACTUAL_KEY])
