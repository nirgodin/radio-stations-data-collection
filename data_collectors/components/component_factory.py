from typing import Optional

from data_collectors.components.deleters.deleters_component_factory import DeletersComponentFactory
from data_collectors.components.environment_component_factory import EnvironmentComponentFactory
from data_collectors.components.exporters.exporters_component_factory import ExportersComponentFactory
from data_collectors.components.managers import *
from data_collectors.components.sessions_component_factory import SessionsComponentFactory
from data_collectors.components.tools_component_factory import ToolsComponentFactory


class ComponentFactory:
    def __init__(self,
                 env: EnvironmentComponentFactory = EnvironmentComponentFactory(),
                 billboard: Optional[BillboardManagerFactory] = None,
                 charts: Optional[ChartsManagerFactory] = None,
                 deleters: Optional[DeletersComponentFactory] = None,
                 exporters: Optional[ExportersComponentFactory] = None,
                 genius: Optional[GeniusManagerFactory] = None,
                 genres: Optional[GenresManagerFactory] = None,
                 google: Optional[GoogleManagerFactory] = None,
                 misc: Optional[MiscellaneousManagerFactory] = None,
                 musixmatch: Optional[MusixmatchManagerFactory] = None,
                 sessions: Optional[SessionsComponentFactory] = None,
                 shazam: Optional[ShazamManagerFactory] = None,
                 spotify: Optional[SpotifyManagerFactory] = None,
                 tools: Optional[ToolsComponentFactory] = None,
                 translation: Optional[TranslationManagerFactory] = None,
                 wikipedia: Optional[WikipediaManagerFactory] = None):
        session_manager = sessions or SessionsComponentFactory(env)
        tools_manager = tools or ToolsComponentFactory(env)

        self.env = env
        self.sessions = session_manager
        self.tools = tools

        self.billboard = billboard or BillboardManagerFactory(
            env=env,
            sessions=session_manager,
            tools=tools_manager,
        )
        self.charts = charts or ChartsManagerFactory(
            env=env,
            sessions=session_manager,
            tools=tools_manager,
        )
        self.deleters = deleters or DeletersComponentFactory()
        self.exporters = exporters or ExportersComponentFactory(
            env=env,
            sessions=session_manager,
            tools=tools_manager,
        )
        self.genius = genius or GeniusManagerFactory(
            env=env,
            sessions=session_manager,
            tools=tools_manager,
        )
        self.genres = genres or GenresManagerFactory(
            env=env,
            sessions=session_manager,
            tools=tools_manager,
        )
        self.google = google or GoogleManagerFactory(
            env=env,
            sessions=session_manager,
            tools=tools_manager,
        )
        self.misc = misc or MiscellaneousManagerFactory(
            env=env,
            sessions=session_manager,
            tools=tools_manager,
        )
        self.musixmatch = musixmatch or MusixmatchManagerFactory(
            env=env,
            sessions=session_manager,
            tools=tools_manager,
        )
        self.shazam = shazam or ShazamManagerFactory(
            env=env,
            sessions=session_manager,
            tools=tools_manager,
        )
        self.spotify = spotify or SpotifyManagerFactory(
            env=env,
            sessions=session_manager,
            tools=tools_manager,
        )
        self.translation = translation or TranslationManagerFactory(
            env=env,
            sessions=session_manager,
            tools=tools_manager,
        )
        self.wikipedia = wikipedia or WikipediaManagerFactory(
            env=env,
            sessions=session_manager,
            tools=tools_manager,
        )
