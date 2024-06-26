from data_collectors.components.collectors.billboard_collectors_component_factory import \
    BillboardCollectorsComponentFactory
from data_collectors.components.collectors.genius_collectors_component_factory import GeniusCollectorsComponentFactory
from data_collectors.components.collectors.google_collectors_component_factory import GoogleCollectorsComponentFactory
from data_collectors.components.collectors.miscellaneous_collectors_factory import MiscellaneousCollectorsFactory
from data_collectors.components.collectors.musixmatch_collectors_component_factory import \
    MusixmatchCollectorsComponentFactory
from data_collectors.components.collectors.openai_collectors_component_factory import OpenAICollectorsComponentFactory
from data_collectors.components.collectors.charts_collectors_component_factory import \
    ChartsCollectorsComponentFactory
from data_collectors.components.collectors.shazam_collectors_component_factory import ShazamCollectorsComponentFactory
from data_collectors.components.collectors.spotify_collectors_component_factory import SpotifyCollectorsComponentFactory
from data_collectors.components.collectors.wikipedia_collectors_component_factory import \
    WikipediaCollectorsComponentFactory


class CollectorsComponentFactory:
    def __init__(self,
                 billboard: BillboardCollectorsComponentFactory = BillboardCollectorsComponentFactory(),
                 charts: ChartsCollectorsComponentFactory = ChartsCollectorsComponentFactory(),
                 genius: GeniusCollectorsComponentFactory = GeniusCollectorsComponentFactory(),
                 google: GoogleCollectorsComponentFactory = GoogleCollectorsComponentFactory(),
                 misc: MiscellaneousCollectorsFactory = MiscellaneousCollectorsFactory(),
                 musixmatch: MusixmatchCollectorsComponentFactory = MusixmatchCollectorsComponentFactory(),
                 openai: OpenAICollectorsComponentFactory = OpenAICollectorsComponentFactory(),
                 shazam: ShazamCollectorsComponentFactory = ShazamCollectorsComponentFactory(),
                 spotify: SpotifyCollectorsComponentFactory = SpotifyCollectorsComponentFactory(),
                 wikipedia: WikipediaCollectorsComponentFactory = WikipediaCollectorsComponentFactory()):
        self.billboard = billboard
        self.charts = charts
        self.genius = genius
        self.google = google
        self.misc = misc
        self.musixmatch = musixmatch
        self.openai = openai
        self.shazam = shazam
        self.spotify = spotify
        self.wikipedia = wikipedia
