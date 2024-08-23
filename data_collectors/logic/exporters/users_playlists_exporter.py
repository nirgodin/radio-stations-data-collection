import math
from typing import Any, List

import pandas as pd
from genie_common.tools import SyncPoolExecutor, logger
from genie_common.utils import safe_nested_get, chain_lists
from genie_datastores.google.sheets import GoogleSheetsUploader, Sheet
from spotipyio import SpotifyClient

from data_collectors.consts.spotify_consts import ITEMS, ID, NAME, TRACKS, SPOTIFY_OPEN_PLAYLIST_URL_FORMAT
from data_collectors.contract import IExporter


class UsersPlaylistsExporter(IExporter):
    def __init__(self,
                 spotify_client: SpotifyClient,
                 google_sheets_uploader: GoogleSheetsUploader,
                 pool_executor: SyncPoolExecutor = SyncPoolExecutor()):
        self._spotify_client = spotify_client
        self._google_sheets_uploader = google_sheets_uploader
        self._pool_executor = pool_executor

    async def export(self, users_ids: List[str], spreadsheet_title: str) -> Any:
        logger.info("Fetching users playlists from spotify with pagination")
        users_playlists = await self._spotify_client.users.playlists.run(
            ids=users_ids,
            max_pages=math.inf
        )
        logger.info("Extracting relevant data from spotify responses")
        records: List[List[dict]] = self._pool_executor.run(
            iterable=users_playlists,
            func=self._serialize_single_user_playlists,
            expected_type=list
        )
        logger.info("Uploading data to google sheets")
        self._upload_data(records, spreadsheet_title)

    @staticmethod
    def _serialize_single_user_playlists(response: dict) -> List[dict]:
        records = []

        for item in response[ITEMS]:
            playlist_id = item[ID]
            record = {
                ID: playlist_id,
                NAME: item[NAME],
                "owner_name": safe_nested_get(item, ["owner", "display_name"]),
                "owner_id": safe_nested_get(item, ["owner", ID]),
                "description": item["description"],
                "total_tracks": safe_nested_get(item, [TRACKS, "total"]),
                "url": SPOTIFY_OPEN_PLAYLIST_URL_FORMAT.format(id=playlist_id)
            }
            records.append(record)

        return records

    def _upload_data(self, records: List[List[dict]], spreadsheet_title: str) -> None:
        flattened_records = chain_lists(records)
        data = pd.DataFrame.from_records(flattened_records)
        sheet = Sheet(data=data, name="users_playlists")
        spreadsheet = self._google_sheets_uploader.upload(
            sheets=[sheet],
            title=spreadsheet_title
        )

        logger.info(f"Successfully uploaded users playlists to the following spreadsheet `{spreadsheet.url}`")
