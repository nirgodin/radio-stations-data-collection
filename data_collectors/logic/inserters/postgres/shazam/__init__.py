from data_collectors.logic.inserters.postgres.shazam.shazam_artists_database_inserter import (
    ShazamArtistsDatabaseInserter
)
from data_collectors.logic.inserters.postgres.shazam.shazam_tracks_database_inserter import ShazamTracksDatabaseInserter
from data_collectors.logic.inserters.postgres.shazam.shazam_top_tracks_database_inserter import (
    ShazamTopTracksDatabaseInserter
)

__all__ = [
    "ShazamTopTracksDatabaseInserter",
    "ShazamTracksDatabaseInserter",
    "ShazamArtistsDatabaseInserter"
]
