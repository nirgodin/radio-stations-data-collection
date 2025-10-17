from datetime import datetime
from typing import List, Dict, Any, Generator
from venv import logger

from genie_common.utils import safe_nested_get
from genie_datastores.models import DataSource
from genie_datastores.postgres.consts.spotify_consts import ADDED_AT, SPOTIFY_DATETIME_FORMAT
from spotipyio import SpotifyClient
from spotipyio.logic.consts.spotify_consts import OWNER, DESCRIPTION

from data_collectors.consts.spotify_consts import TRACKS, ITEMS, TRACK, ID, DISPLAY_NAME, NAME, SNAPSHOT_ID
from data_collectors.contract import ICollector
from data_collectors.logic.models import Curation


class SpotifyPlaylistsCurationsCollector(ICollector):
    def __init__(self, spotify_client: SpotifyClient):
        self._spotify_client = spotify_client

    async def collect(self, playlists_ids: List[str]) -> List[Curation]:
        playlists = await self._spotify_client.playlists.info.run(
            ids=playlists_ids,
            max_pages=30,
        )
        curations = []

        for playlist in playlists:
            playlist_curations = self._build_single_playlist_curations(playlist)
            curations.extend(playlist_curations)

        return curations

    def _build_single_playlist_curations(self, playlist: Dict[str, Any]) -> List[Curation]:
        tracks = safe_nested_get(playlist, [TRACKS, ITEMS], [])
        if not tracks:
            return []

        tracks_ids = self._extract_playlist_tracks(tracks)
        update_date = self._extract_playlist_update_date(tracks)

        return list(self._to_curations(playlist, tracks_ids, update_date))

    @staticmethod
    def _extract_playlist_tracks(tracks: List[Dict[str, Any]]) -> List[str]:
        tracks_ids = [safe_nested_get(track, [TRACK, ID]) for track in tracks]
        return [id_ for id_ in tracks_ids if isinstance(id_, str)]

    @staticmethod
    def _extract_playlist_update_date(tracks: List[Dict[str, Any]]) -> datetime:
        tracks_add_date = [track.get(ADDED_AT) for track in tracks]
        valid_add_dates = [added_date for added_date in tracks_add_date if isinstance(added_date, str)]

        if not valid_add_dates:
            logger.warning("Fail to extract any track add date. Using datetime.now as fallback")
            return datetime.now()

        sorted_dates = sorted(valid_add_dates, reverse=True)
        last_add_date = sorted_dates[0]

        return datetime.strptime(last_add_date, SPOTIFY_DATETIME_FORMAT)

    @staticmethod
    def _to_curations(
        playlist: Dict[str, Any], tracks_ids: List[str], update_date: datetime
    ) -> Generator[Curation, None, None]:
        curator_id = safe_nested_get(playlist, [OWNER, ID])
        collection_id = playlist[ID]
        curator_name = safe_nested_get(playlist, [OWNER, DISPLAY_NAME])
        collection_name = playlist[NAME]
        collection_description = playlist[DESCRIPTION]
        snapshot_id = playlist[SNAPSHOT_ID]

        for track_id in tracks_ids:
            yield Curation(
                collection_id=collection_id,
                curator_id=curator_id,
                curator_name=curator_name,
                date=update_date,
                description=collection_description,
                source=DataSource.SPOTIFY,
                title=collection_name,
                track_id=track_id,
                collection_comment=snapshot_id,
            )
