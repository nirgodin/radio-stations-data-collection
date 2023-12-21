from data_collectors.components.environment_component_factory import EnvironmentComponentFactory
from data_collectors.components.managers import *
from data_collectors.components.sessions_component_factory import SessionsComponentFactory
from data_collectors.components.tools_component_factory import ToolsComponentFactory


class ComponentFactory:
    def __init__(self,
                 billboard: BillboardManagerFactory = BillboardManagerFactory(),
                 env: EnvironmentComponentFactory = EnvironmentComponentFactory(),
                 genius: GeniusManagerFactory = GeniusManagerFactory(),
                 misc: MiscellaneousManagerFactory = MiscellaneousManagerFactory(),
                 musixmatch: MusixmatchManagerFactory = MusixmatchManagerFactory(),
                 sessions: SessionsComponentFactory = SessionsComponentFactory(),
                 shazam: ShazamManagerFactory = ShazamManagerFactory(),
                 spotify: SpotifyManagerFactory = SpotifyManagerFactory(),
                 tools: ToolsComponentFactory = ToolsComponentFactory(),
                 wikipedia: WikipediaManagerFactory = WikipediaManagerFactory()):
        self.billboard = billboard
        self.env = env
        self.genius = genius
        self.misc = misc
        self.musixmatch = musixmatch
        self.sessions = sessions
        self.shazam = shazam
        self.spotify = spotify
        self.tools = tools
        self.wikipedia = wikipedia
