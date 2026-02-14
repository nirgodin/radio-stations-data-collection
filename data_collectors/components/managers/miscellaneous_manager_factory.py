from aiohttp import ClientSession
from genie_datastores.models import DataSource
from genie_datastores.postgres.models import TrackIDMapping
from genie_datastores.postgres.operations import get_database_engine
from spotipyio.auth import SpotifySession

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *
from data_collectors.logic.models import LyricsSourceDetails


class MiscellaneousManagerFactory(BaseManagerFactory):
    def get_radio_snapshots_manager(self, spotify_session: SpotifySession) -> RadioStationsSnapshotsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)

        return RadioStationsSnapshotsManager(
            spotify_client=spotify_client,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            radio_tracks_database_inserter=self.inserters.get_radio_tracks_inserter(),
        )

    def get_tracks_lyrics_manager(self, session: ClientSession) -> TracksLyricsManager:
        prioritized_sources = [
            LyricsSourceDetails(
                column=TrackIDMapping.genius_id,
                collector=self.collectors.genius.get_lyrics_collector(session),
                data_source=DataSource.GENIUS,
            ),
        ]

        return TracksLyricsManager(
            db_engine=self.tools.get_database_engine(),
            prioritized_sources=prioritized_sources,
            lyrics_formatter=self.collectors.misc.get_tracks_lyrics_formatter(),
            chunks_database_inserter=self.tools.get_chunks_database_inserter(),
        )

    def get_lyrics_missing_ids_manager(self) -> TracksLyricsMissingIDsManager:
        return TracksLyricsMissingIDsManager(
            db_engine=get_database_engine(),
            chunks_inserter=self.tools.get_chunks_database_inserter(),
        )

    def get_release_radar_manager(self, spotify_authorized_session: SpotifySession) -> ReleaseRadarManager:
        return ReleaseRadarManager(
            db_engine=get_database_engine(),
            spotify_client=self.tools.get_spotify_client(spotify_authorized_session),
            playlist_id=self.env.get_release_radar_playlist_id(),
        )

    def get_status_reporter_manager(self) -> StatusReporterManager:
        collectors = [
            self.collectors.misc.get_radio_tracks_status_collector(),
            self.collectors.misc.get_shazam_top_tracks_status_collector(),
            self.collectors.misc.get_radio_tracks_top_tracks_status_collector(),
            self.collectors.misc.get_charts_count_status_collector(),
            self.collectors.misc.get_charts_top_tracks_status_collector(),
        ]
        return StatusReporterManager(
            collectors=collectors,
            pool_executor=self.tools.get_pool_executor(),
            email_sender=self.tools.get_email_sender(),
            recipients=[self.env.get_email_user()],
        )
