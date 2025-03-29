from functools import partial
from typing import Dict, List, Callable, Optional
from urllib.parse import urlparse

from data_collectors.contract import ISerializer
from data_collectors.logic.models import SpotifyArtistAbout


class SpotifyArtistAboutSerializer(ISerializer):
    def serialize(self, artist_id: str, artist_name: str, details: List[Dict[str, str]]) -> SpotifyArtistAbout:
        artist_about = SpotifyArtistAbout(id=artist_id, name=artist_name)

        for (
            field_name,
            extraction_method,
        ) in self._field_name_to_extraction_method.items():
            extracted_value = extraction_method(details)

            if extracted_value is not None:
                setattr(artist_about, field_name, extracted_value)

        return artist_about

    @staticmethod
    def _extract_url_path(key: str, details: List[Dict[str, str]]) -> Optional[str]:
        for detail in details:
            url = detail.get(key)

            if url:
                parsed_url = urlparse(url)
                return parsed_url.path.strip("/")

    @staticmethod
    def _extract_artist_about(details: List[Dict[str, str]]) -> Optional[str]:
        paragraphs = []

        for detail in details:
            paragraph = detail.get("about")

            if paragraph:
                paragraphs.append(paragraph)

        if paragraphs:
            return "\n".join(paragraphs)

    @property
    def _field_name_to_extraction_method(
        self,
    ) -> Dict[str, Callable[[List[Dict[str, str]]], Optional[str]]]:
        return {
            "facebook_name": partial(self._extract_url_path, "Facebook"),
            "twitter_name": partial(self._extract_url_path, "Twitter"),
            "instagram_name": partial(self._extract_url_path, "Instagram"),
            "about": self._extract_artist_about,
        }
