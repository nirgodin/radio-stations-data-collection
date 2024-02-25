from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.serializers import *


class SerializersComponentFactory:
    def __init__(self, tools: ToolsComponentFactory = ToolsComponentFactory()):
        self.tools = tools

    def get_tracks_lyrics_serializer(self) -> TracksLyricsSerializer:
        return TracksLyricsSerializer(self.tools.get_language_identifier())
