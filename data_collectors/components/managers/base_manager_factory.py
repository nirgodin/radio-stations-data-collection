from abc import ABC

from typing_extensions import Optional

from data_collectors.components.analyzers.analyzers_component_factory import AnalyzersComponentFactory
from data_collectors.components.collectors import CollectorsComponentFactory
from data_collectors.components.environment_component_factory import EnvironmentComponentFactory
from data_collectors.components.inserters import InsertersComponentFactory
from data_collectors.components.serializers.serializers_component_factory import SerializersComponentFactory
from data_collectors.components.sessions_component_factory import SessionsComponentFactory
from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.components.updaters import UpdatersComponentFactory


class BaseManagerFactory(ABC):
    def __init__(self,
                 env: EnvironmentComponentFactory,
                 sessions: SessionsComponentFactory,
                 tools: ToolsComponentFactory,
                 analyzers: Optional[AnalyzersComponentFactory] = None,
                 collectors: Optional[CollectorsComponentFactory] = None,
                 inserters: Optional[InsertersComponentFactory] = None,
                 serializers: Optional[SerializersComponentFactory] = None,
                 updaters: Optional[UpdatersComponentFactory] = None):
        self.env = env
        self.sessions = sessions
        self.tools = tools

        self.analyzers = analyzers or AnalyzersComponentFactory()
        self.collectors = collectors or CollectorsComponentFactory(tools)
        self.inserters = inserters or InsertersComponentFactory(tools)
        self.serializers = serializers or SerializersComponentFactory(tools)
        self.updaters = updaters or UpdatersComponentFactory(tools)
