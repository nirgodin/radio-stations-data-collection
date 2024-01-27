from abc import ABC

from data_collectors.components.analyzers.analyzers_component_factory import AnalyzersComponentFactory
from data_collectors.components.collectors import CollectorsComponentFactory
from data_collectors.components.environment_component_factory import EnvironmentComponentFactory
from data_collectors.components.inserters import InsertersComponentFactory
from data_collectors.components.sessions_component_factory import SessionsComponentFactory
from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.components.updaters import UpdatersComponentFactory


class BaseManagerFactory(ABC):
    def __init__(self,
                 analyzers: AnalyzersComponentFactory = AnalyzersComponentFactory(),
                 collectors: CollectorsComponentFactory = CollectorsComponentFactory(),
                 inserters: InsertersComponentFactory = InsertersComponentFactory(),
                 updaters: UpdatersComponentFactory = UpdatersComponentFactory(),
                 env: EnvironmentComponentFactory = EnvironmentComponentFactory(),
                 sessions: SessionsComponentFactory = SessionsComponentFactory(),
                 tools: ToolsComponentFactory = ToolsComponentFactory()):
        self.analyzers = analyzers
        self.collectors = collectors
        self.inserters = inserters
        self.updaters = updaters
        self.env = env
        self.sessions = sessions
        self.tools = tools
