from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.serializers import *


class SerializersComponentFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self._tools = tools

    def get_tracks_lyrics_serializer(self) -> TracksLyricsSerializer:
        return TracksLyricsSerializer(self._tools.get_language_identifier())

    def get_artists_about_paragraphs_serializer(
        self,
    ) -> ArtistsAboutParagraphsSerializer:
        return ArtistsAboutParagraphsSerializer(generative_model=self._tools.get_gemini_model())

    @staticmethod
    def get_artists_web_pages_serializer() -> ArtistsWebPagesSerializer:
        return ArtistsWebPagesSerializer(similarity_threshold=0.65)
