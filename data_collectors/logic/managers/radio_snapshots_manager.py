from typing import List

from genie_datastores.postgres.models import SpotifyArtist, SpotifyStation
from spotipyio.logic.spotify_client import SpotifyClient

from data_collectors.logic.inserters import SpotifyInsertionsManager, RadioTracksDatabaseInserter
from data_collectors.consts.spotify_consts import ID, TRACKS, ARTISTS, ITEMS
from data_collectors.contract import IManager
from data_collectors.logs import logger


class RadioStationsSnapshotsManager(IManager):
    def __init__(self,
                 spotify_client: SpotifyClient,
                 spotify_insertions_manager: SpotifyInsertionsManager,
                 radio_tracks_database_inserter: RadioTracksDatabaseInserter):
        self._spotify_client = spotify_client
        self._spotify_insertions_manager = spotify_insertions_manager
        self._radio_tracks_database_inserter = radio_tracks_database_inserter

    async def run(self, stations: List[SpotifyStation]) -> None:
        logger.info('Starting to run `RadioStationsSnapshotsCollector`')
        playlists_ids = [station.value for station in stations]
        playlists = await self._spotify_client.playlists.collect(playlists_ids)
        await self._insert_records_to_db(playlists)
        logger.info('Successfully collected and inserted playlists to DB')

    async def _insert_records_to_db(self, playlists: List[dict]) -> None:
        for playlist in playlists:
            logger.info(f'Starting to insert playlist `{playlist[ID]}` spotify records')
            tracks = playlist[TRACKS][ITEMS]
            spotify_records = await self._spotify_insertions_manager.insert(tracks)
            await self._insert_radio_tracks(
                playlist=playlist,
                tracks=tracks,
                artists=spotify_records[ARTISTS]
            )

    async def _insert_radio_tracks(self, playlist: dict, tracks: List[dict], artists: List[SpotifyArtist]) -> None:
        artists_ids = [artist.id for artist in artists]
        artists_responses = await self._spotify_client.artists.info.collect(artists_ids)

        await self._radio_tracks_database_inserter.insert(
            playlist=playlist,
            tracks=tracks,
            artists=artists_responses
        )
