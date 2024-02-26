from typing import Optional

from genie_common.tools import SyncPoolExecutor, logger
from genie_common.utils import compute_similarity_score
from genie_datastores.google.sheets import GoogleSheetsUploader, Sheet
from genie_datastores.postgres.models import SpotifyTrack, SpotifyArtist, ShazamTrack, ShazamArtist, TrackIDMapping
from genie_datastores.postgres.operations import read_sql
from pandas import DataFrame, Series
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.general_consts import SIMILARITY
from data_collectors.consts.shazam_matches_exporter_consts import QUERY_COLUMNS, SPOTIFY_KEY_COLUMN, \
    SPOTIFY_TRACK_NAME_COLUMN, SPOTIFY_ARTIST_NAME_COLUMN, SHAZAM_KEY_COLUMN, SHAZAM_ARTIST_NAME_COLUMN, \
    SHAZAM_TRACK_NAME_COLUMN
from data_collectors.contract.exporter_interface import IExporter


class ShazamMatchesExporter(IExporter):
    def __init__(self,
                 db_engine: AsyncEngine,
                 sheets_uploader: GoogleSheetsUploader,
                 pool_executor: SyncPoolExecutor = SyncPoolExecutor()):
        self._db_engine = db_engine
        self._sheets_uploader = sheets_uploader
        self._pool_executor = pool_executor

    async def export(self, limit: Optional[int]) -> None:
        logger.info("Starting to run `ShazamMatchesExporter`")
        data = await self._query_spotify_to_shazam_tracks_mapping(limit)
        self._pre_process_output_data(data)
        spreadsheet = self._sheets_uploader.upload(
            title="Shazam Spotify Matches Comparison",
            sheets=[Sheet(data, "matches comparison")]
        )
        logger.info(f"Exported data to sheet with url `{spreadsheet.url}`")

    async def _query_spotify_to_shazam_tracks_mapping(self, limit: Optional[int]) -> DataFrame:
        logger.info("Querying comparison data from db")
        non_null_condition = and_(
            SpotifyTrack.name.isnot(None),
            SpotifyArtist.name.isnot(None),
            ShazamTrack.name.isnot(None),
            ShazamArtist.name.isnot(None),
        )
        query = (
            select(*QUERY_COLUMNS)
            .where(SpotifyArtist.id == SpotifyTrack.artist_id)
            .where(SpotifyTrack.id == TrackIDMapping.id)
            .where(TrackIDMapping.shazam_id == ShazamTrack.id)
            .where(ShazamTrack.artist_id == ShazamArtist.id)
            .where(non_null_condition)
            .limit(limit)
        )
        data = await read_sql(engine=self._db_engine, query=query)

        return data.fillna("")

    def _pre_process_output_data(self, data: DataFrame) -> None:
        logger.info("Pre processing data")
        data[SPOTIFY_KEY_COLUMN] = data[SPOTIFY_ARTIST_NAME_COLUMN] + " - " + data[SPOTIFY_TRACK_NAME_COLUMN]
        data[SHAZAM_KEY_COLUMN] = data[SHAZAM_ARTIST_NAME_COLUMN] + " - " + data[SHAZAM_TRACK_NAME_COLUMN]
        similarities = self._pool_executor.run(
            iterable=[row for _, row in data.iterrows()],
            func=self._compute_keys_similarity,
            expected_type=float
        )
        data[SIMILARITY] = similarities
        data.sort_values(by=SIMILARITY, ascending=True, inplace=True)

    @staticmethod
    def _compute_keys_similarity(row: Series) -> float:
        return compute_similarity_score(row[SPOTIFY_KEY_COLUMN], row[SHAZAM_KEY_COLUMN])
