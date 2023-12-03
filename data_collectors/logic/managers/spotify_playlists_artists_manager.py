from typing import List, Generator, Set, Any

from genie_common.tools import logger
from genie_common.utils import safe_nested_get
from genie_datastores.postgres.models import SpotifyArtist
from spotipyio import SpotifyClient
from tqdm import tqdm

from data_collectors.consts.spotify_consts import ID, TRACKS, ITEMS, TRACK
from data_collectors.contract import IManager
from genie_datastores.postgres.inner_utils.spotify_utils import extract_artist_id

from data_collectors.logic.models import ArtistUpdateRequest
from data_collectors.logic.updaters.spotify_artists_database_updater import SpotifyArtistsDatabaseUpdater


class SpotifyPlaylistsArtistsManager(IManager):
    def __init__(self, spotify_client: SpotifyClient, artists_updater: SpotifyArtistsDatabaseUpdater):
        self._spotify_client = spotify_client
        self._artists_updater = artists_updater

    async def run(self, playlists_ids: List[str], key_column: SpotifyArtist, value: Any) -> None:
        playlists = await self._spotify_client.playlists.collect(playlists_ids)
        artists_ids = self._extract_playlists_artists(playlists)
        update_requests = [ArtistUpdateRequest(artist_id=id_, values={key_column: value}) for id_ in artists_ids]

        await self._artists_updater.update(update_requests)

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
                artist_id = extract_artist_id(inner_track)
                progress_bar.update(1)

                yield artist_id
