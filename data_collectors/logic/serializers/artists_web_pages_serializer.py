from datetime import datetime
from typing import Optional, Dict, List
from urllib.parse import urlparse, ParseResult, unquote

from genie_common.tools import logger
from genie_common.utils import compute_similarity_score
from genie_datastores.postgres.models import SpotifyArtist

from data_collectors.contract import ISerializer
from data_collectors.logic.models import DBUpdateRequest, DomainExtractSettings, Domain


class ArtistsWebPagesSerializer(ISerializer):
    def __init__(self, similarity_threshold: float):
        self._similarity_threshold = similarity_threshold

    def serialize(
        self, ids_artists_map: Dict[str, SpotifyArtist], ids_web_pages_map: Dict[str, Dict[Domain, str]]
    ) -> List[DBUpdateRequest]:
        logger.info(f"Building {len(ids_web_pages_map)} update requests")
        requests = []

        for artist_id, web_pages in ids_web_pages_map.items():
            artist = ids_artists_map[artist_id]
            update_request = self._to_update_request(artist, web_pages)
            requests.append(update_request)

        return requests

    def _to_update_request(self, artist: SpotifyArtist, web_pages: Dict[Domain, str]) -> DBUpdateRequest:
        update_values = {SpotifyArtist.update_date: datetime.utcnow()}

        for domain, link in web_pages.items():
            parse_result = urlparse(link)
            extract_settings = self._domain_extract_function_map[domain]

            for setting in extract_settings:
                extracted_value = self._apply_single_extraction(
                    extraction_setting=setting, artist=artist, parse_result=parse_result
                )

                if extracted_value is not None:
                    update_values[setting.column] = extracted_value

        return DBUpdateRequest(id=artist.id, values=update_values)

    def _apply_single_extraction(
        self, extraction_setting: DomainExtractSettings, artist: SpotifyArtist, parse_result: ParseResult
    ) -> Optional[str]:
        if self._is_missing_column(extraction_setting, artist):
            return extraction_setting.extract_fn(parse_result, artist.name)

    @staticmethod
    def _is_missing_column(extract_settings: DomainExtractSettings, artist: SpotifyArtist) -> bool:
        existing_value = getattr(artist, extract_settings.column.key)
        return existing_value is None

    def _extract_wiki_name(self, parse_result: ParseResult, artist_name: str) -> Optional[str]:
        formatted_path = parse_result.path.replace("wiki", "").strip("/")
        wiki_name = unquote(formatted_path)

        if self._is_matching_value(wiki_name, artist_name):
            return unquote(formatted_path)

    def _extract_wiki_language(self, parse_result: ParseResult, artist_name: str) -> Optional[str]:
        wiki_name = self._extract_wiki_name(parse_result, artist_name)

        if wiki_name:
            return parse_result.hostname.split(".")[0]

    def _extract_domain_route(self, parse_result: ParseResult, artist_name: str) -> Optional[str]:
        domain_route = parse_result.path.strip("/")

        if self._is_matching_value(domain_route, artist_name):
            return domain_route

    def _is_matching_value(self, web_page_name: str, artist_name: str) -> bool:
        similarity_score = compute_similarity_score(web_page_name, artist_name)
        return similarity_score > self._similarity_threshold

    @property
    def _domain_extract_function_map(self) -> Dict[Domain, List[DomainExtractSettings]]:
        domain_extract_map = {}

        for domain_extract in self._domain_extract_functions:
            if domain_extract.domain not in domain_extract_map.keys():
                domain_extract_map[domain_extract.domain] = []

            domain_extract_map[domain_extract.domain].append(domain_extract)

        return domain_extract_map

    @property
    def _domain_extract_functions(self) -> List[DomainExtractSettings]:
        return [
            DomainExtractSettings(
                domain=Domain.WIKIPEDIA,
                extract_fn=self._extract_wiki_name,
                column=SpotifyArtist.wikipedia_name,
            ),
            DomainExtractSettings(
                domain=Domain.WIKIPEDIA,
                extract_fn=self._extract_wiki_language,
                column=SpotifyArtist.wikipedia_language,
            ),
            DomainExtractSettings(
                domain=Domain.FACEBOOK,
                extract_fn=self._extract_domain_route,
                column=SpotifyArtist.facebook_name,
            ),
            DomainExtractSettings(
                domain=Domain.INSTAGRAM,
                extract_fn=self._extract_domain_route,
                column=SpotifyArtist.instagram_name,
            ),
            DomainExtractSettings(
                domain=Domain.TWITTER,
                extract_fn=self._extract_domain_route,
                column=SpotifyArtist.twitter_name,
            ),
        ]
