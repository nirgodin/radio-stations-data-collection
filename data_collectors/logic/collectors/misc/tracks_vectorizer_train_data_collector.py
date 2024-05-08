from typing import List

from genie_common.tools import logger
from genie_datastores.postgres.models import AudioFeatures, SpotifyTrack, RadioTrack, BaseORMModel
from genie_datastores.postgres.operations import read_sql
from pandas import DataFrame
from sqlalchemy import select, func, extract
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.musixmatch_consts import TRACK_ID
from data_collectors.consts.spotify_consts import ID
from data_collectors.contract import ICollector


class TracksVectorizerTrainDataCollector(ICollector):
    def __init__(self, db_engine: AsyncEngine):
        self._db_engine = db_engine

    async def collect(self) -> DataFrame:
        radio_tracks_data = await self._query_radio_tracks_data()
        tracks_data = await self._query_tracks_data()
        logger.info("Merging tracks and radio tracks data")
        merged_data = radio_tracks_data.merge(
            right=tracks_data,
            how='left',
            left_on=TRACK_ID,
            right_on=ID,
        )

        return merged_data.drop(ID, axis=1)

    async def _query_radio_tracks_data(self) -> DataFrame:
        logger.info("Querying radio tracks data")
        query = (
            select(*self._radio_tracks_query_columns)
            .group_by(RadioTrack.track_id)
        )
        return await read_sql(engine=self._db_engine, query=query)

    async def _query_tracks_data(self) -> DataFrame:
        logger.info("Querying tracks data")
        query = (
            select(*self._tracks_query_columns)
            .where(SpotifyTrack.id == AudioFeatures.id)
        )
        return await read_sql(engine=self._db_engine, query=query)

    @property
    def _radio_tracks_query_columns(self) -> List[BaseORMModel]:
        return [
            RadioTrack.track_id,
            func.avg(RadioTrack.popularity).label("popularity"),
            func.avg(RadioTrack.artist_popularity).label("artist_popularity"),
            func.avg(RadioTrack.artist_followers).label("artist_followers"),
        ]

    @property
    def _tracks_query_columns(self) -> List[BaseORMModel]:
        return [
            AudioFeatures.danceability,
            AudioFeatures.energy,
            AudioFeatures.speechiness,
            AudioFeatures.acousticness,
            AudioFeatures.instrumentalness,
            AudioFeatures.liveness,
            AudioFeatures.valence,
            AudioFeatures.mode,
            AudioFeatures.key,
            SpotifyTrack.id,
            SpotifyTrack.number.label("track_number"),
            SpotifyTrack.explicit,
            AudioFeatures.loudness,
            AudioFeatures.tempo,
            AudioFeatures.time_signature,
            extract("year", SpotifyTrack.release_date).label("release_year"),
            AudioFeatures.duration_ms,
        ]
