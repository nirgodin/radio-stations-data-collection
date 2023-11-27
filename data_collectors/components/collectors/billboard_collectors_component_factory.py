from aiohttp import ClientSession
from spotipyio import SpotifyClient

from data_collectors.logic.collectors import BillboardChartsCollector, BillboardTracksCollector


class BillboardCollectorsComponentFactory:
    @staticmethod
    def get_charts_collector(session: ClientSession) -> BillboardChartsCollector:
        return BillboardChartsCollector(session)

    @staticmethod
    def get_tracks_collector(session: ClientSession, spotify_client: SpotifyClient) -> BillboardTracksCollector:
        return BillboardTracksCollector(session, spotify_client)
