from genie_datastores.google_drive.google_drive_client import GoogleDriveClient

from data_collectors.logic.collectors import RadioChartsDataCollector


class RadioChartsCollectorsComponentFactory:
    @staticmethod
    def get_charts_collector(google_drive_client: GoogleDriveClient) -> RadioChartsDataCollector:
        return RadioChartsDataCollector(google_drive_client)
