from typing import List

from genie_datastores.postgres.inner_utils.spotify_utils import extract_artist_id
from genie_datastores.postgres.models import SpotifyArtist, SpotifyStation, RadioTrack
from spotipyio import SpotifyClient

from data_collectors.logic.inserters.postgres import SpotifyInsertionsManager, RadioTracksDatabaseInserter
from data_collectors.consts.spotify_consts import ID, TRACKS, ARTISTS, ITEMS, TRACK
from data_collectors.contract import IManager
from genie_common.tools import logger


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
        playlists = await self._spotify_client.playlists.info.run(playlists_ids)
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
        artists_ids = sorted([artist.id for artist in artists])
        artists_responses = await self._spotify_client.artists.info.run(artists_ids)
        records = self._to_records(
            playlist=playlist,
            tracks=tracks,
            artists=artists_responses
        )

        await self._radio_tracks_database_inserter.insert(records)

    def _to_records(self, playlist: dict, tracks: List[dict], artists: List[dict]) -> List[RadioTrack]:
        records = []

        for track in tracks:
            artist = self._extract_artist_details(track, artists)
            record = RadioTrack.from_playlist_artist_track(
                playlist=playlist,
                artist=artist,
                track=track
            )
            records.append(record)

        return records

    @staticmethod
    def _extract_artist_details(track: dict, artists: List[dict]) -> dict:
        artist_id = extract_artist_id(track[TRACK])

        for artist in artists:
            if artist[ID] == artist_id:
                return artist
