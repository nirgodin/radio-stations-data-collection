from typing import List, Generator, Set, Any, Dict

from genie_common.tools import logger
from genie_common.utils import safe_nested_get
from genie_datastores.postgres.inner_utils.spotify_utils import extract_artist_id
from genie_datastores.postgres.models import Artist
from spotipyio import SpotifyClient
from tqdm import tqdm

from data_collectors.consts.spotify_consts import ID, TRACKS, ITEMS, TRACK
from data_collectors.contract import IManager
from data_collectors.logic.models import DBUpdateRequest
from data_collectors.logic.updaters.values_database_updater import ValuesDatabaseUpdater


class SpotifyPlaylistsArtistsManager(IManager):
    def __init__(self, spotify_client: SpotifyClient, db_updater: ValuesDatabaseUpdater):
        self._spotify_client = spotify_client
        self._db_updater = db_updater

    async def run(self, playlists_ids: List[str], values: Dict[Artist, Any]) -> None:
        playlists = await self._spotify_client.playlists.info.run(playlists_ids)
        valid_playlists = self._filter_out_invalid_playlists(playlists)
        artists_ids = self._extract_playlists_artists(valid_playlists)
        update_requests = [DBUpdateRequest(id=id_, values=values) for id_ in artists_ids]

        await self._db_updater.update(update_requests)

    @staticmethod
    def _filter_out_invalid_playlists(playlists: List[dict]) -> List[dict]:
        valid_playlists = [playlist for playlist in playlists if isinstance(playlist, dict)]
        n_playlists = len(playlists)
        n_valid_playlists = len(valid_playlists)

        if n_valid_playlists < n_playlists:
            n_invalid_playlists = n_playlists - n_valid_playlists
            logger.warn(f"Found {n_invalid_playlists} invalid playlists. Filtering them out")

        return valid_playlists

    def _extract_playlists_artists(self, playlists: List[dict],) -> Set[str]:
        artists_ids = set()

        for playlist in playlists:
            logger.info(f'Starting to extract playlist `{playlist[ID]}` artists')

            for artist_id in self._extract_single_playlist_main_artists(playlist):
                artists_ids.add(artist_id)

        return artists_ids

    @staticmethod
    def _extract_single_playlist_main_artists(playlist: dict) -> Generator[str, None, None]:
        playlist_tracks = safe_nested_get(playlist, [TRACKS, ITEMS], [])

        with tqdm(total=len(playlist_tracks)) as progress_bar:
            for track in playlist_tracks:
                inner_track = track.get(TRACK, {})

                if inner_track:
                    artist_id = extract_artist_id(inner_track)
                    progress_bar.update(1)

                    yield artist_id
