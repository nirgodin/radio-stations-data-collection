from dataclasses import dataclass

from genie_datastores.postgres.models import TrackIDMapping
from genie_datastores.models import DataSource

from data_collectors.contract.collectors.lyrics_collector_interface import ILyricsCollector


@dataclass
class LyricsSourceDetails:
    column: TrackIDMapping
    collector: ILyricsCollector
    data_source: DataSource
