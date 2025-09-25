from typing import Optional

from data_collectors.components.collectors.genius_collectors_component_factory import (
    GeniusCollectorsComponentFactory,
)
from data_collectors.components.collectors.google_collectors_component_factory import (
    GoogleCollectorsComponentFactory,
)
from data_collectors.components.collectors.miscellaneous_collectors_factory import (
    MiscellaneousCollectorsFactory,
)
from data_collectors.components.collectors.musixmatch_collectors_component_factory import (
    MusixmatchCollectorsComponentFactory,
)
from data_collectors.components.collectors.openai_collectors_component_factory import (
    OpenAICollectorsComponentFactory,
)
from data_collectors.components.collectors.charts_collectors_component_factory import (
    ChartsCollectorsComponentFactory,
)
from data_collectors.components.collectors.shazam_collectors_component_factory import (
    ShazamCollectorsComponentFactory,
)
from data_collectors.components.collectors.spotify_collectors_component_factory import (
    SpotifyCollectorsComponentFactory,
)
from data_collectors.components.collectors.wikipedia_collectors_component_factory import (
    WikipediaCollectorsComponentFactory,
)
from data_collectors.components.environment_component_factory import EnvironmentComponentFactory
from data_collectors.components.tools_component_factory import ToolsComponentFactory


class CollectorsComponentFactory:
    def __init__(
        self,
        env: EnvironmentComponentFactory,
        tools: ToolsComponentFactory,
        charts: Optional[ChartsCollectorsComponentFactory] = None,
        genius: Optional[GeniusCollectorsComponentFactory] = None,
        google: Optional[GoogleCollectorsComponentFactory] = None,
        misc: Optional[MiscellaneousCollectorsFactory] = None,
        musixmatch: Optional[MusixmatchCollectorsComponentFactory] = None,
        openai: Optional[OpenAICollectorsComponentFactory] = None,
        shazam: Optional[ShazamCollectorsComponentFactory] = None,
        spotify: Optional[SpotifyCollectorsComponentFactory] = None,
        wikipedia: Optional[WikipediaCollectorsComponentFactory] = None,
    ):
        self.charts = charts or ChartsCollectorsComponentFactory(env, tools)
        self.genius = genius or GeniusCollectorsComponentFactory(tools)
        self.google = google or GoogleCollectorsComponentFactory(tools)
        self.misc = misc or MiscellaneousCollectorsFactory(tools)
        self.musixmatch = musixmatch or MusixmatchCollectorsComponentFactory(tools)
        self.openai = openai or OpenAICollectorsComponentFactory(tools)
        self.shazam = shazam or ShazamCollectorsComponentFactory(tools)
        self.spotify = spotify or SpotifyCollectorsComponentFactory(tools)
        self.wikipedia = wikipedia or WikipediaCollectorsComponentFactory(tools)
