from data_collectors.components.collectors.billboard_collectors_component_factory import \
    BillboardCollectorsComponentFactory
from data_collectors.components.collectors.genius_collectors_component_factory import GeniusCollectorsComponentFactory
from data_collectors.components.collectors.musixmatch_collectors_component_factory import \
    MusixmatchCollectorsComponentFactory
from data_collectors.components.collectors.openai_collectors_component_factory import OpenAICollectorsComponentFactory
from data_collectors.components.collectors.shazam_collectors_component_factory import ShazamCollectorsComponentFactory
from data_collectors.components.collectors.spotify_collectors_component_factory import SpotifyCollectorsComponentFactory
from data_collectors.components.collectors.wikipedia_collectors_component_factory import \
    WikipediaCollectorsComponentFactory


class CollectorsComponentFactory:
    def __init__(self,
                 billboard: BillboardCollectorsComponentFactory = BillboardCollectorsComponentFactory(),
                 shazam: ShazamCollectorsComponentFactory = ShazamCollectorsComponentFactory(),
                 musixmatch: MusixmatchCollectorsComponentFactory = MusixmatchCollectorsComponentFactory(),
                 genius: GeniusCollectorsComponentFactory = GeniusCollectorsComponentFactory(),
                 openai: OpenAICollectorsComponentFactory = OpenAICollectorsComponentFactory(),
                 spotify: SpotifyCollectorsComponentFactory = SpotifyCollectorsComponentFactory(),
                 wikipedia: WikipediaCollectorsComponentFactory = WikipediaCollectorsComponentFactory()):
        self.billboard = billboard
        self.shazam = shazam
        self.musixmatch = musixmatch
        self.genius = genius
        self.openai = openai
        self.spotify = spotify
        self.wikipedia = wikipedia
