from statistics import mean
from typing import List, Dict, Union, Optional

from data_collectors.consts.musixmatch_consts import MESSAGE, BODY, TRACK_LIST, HEADER, STATUS_CODE, OK_STATUS_CODE, \
    TRACK_NAME, ARTIST_NAME, TRACK_ID
from data_collectors.consts.spotify_consts import TRACK
from data_collectors.logic.models import MissingTrack
from data_collectors.logic.utils import safe_nested_get
from data_collectors.logic.utils import compute_similarity_score


class MusixmatchSearchResultsExtractor:
    @staticmethod
    def extract(response: dict, missing_track: MissingTrack) -> Optional[str]:
        track_list = MusixmatchSearchResultsExtractor._get_response_track_list(response)
        if not track_list:
            return

        most_similar_track = MusixmatchSearchResultsExtractor._extract_most_similar_track(track_list, missing_track)
        track_id = safe_nested_get(most_similar_track, [TRACK, TRACK_ID])

        if track_id is not None:
            return str(track_id)

    @staticmethod
    def _get_response_track_list(response: dict) -> list:
        status_code = safe_nested_get(response, [MESSAGE, HEADER, STATUS_CODE])

        if status_code != OK_STATUS_CODE:
            return []

        return safe_nested_get(response, [MESSAGE, BODY, TRACK_LIST], default=[])

    @staticmethod
    def _extract_most_similar_track(track_list: List[dict], missing_track: MissingTrack) -> dict:
        if len(track_list) == 1:
            return track_list[0]

        tracks_similarity_scores = MusixmatchSearchResultsExtractor._get_tracks_similarity_scores(
            track_list=track_list,
            missing_track=missing_track
        )
        most_similar_track_index = max(tracks_similarity_scores, key=tracks_similarity_scores.get)

        return track_list[most_similar_track_index]

    @staticmethod
    def _get_tracks_similarity_scores(track_list: List[dict], missing_track: MissingTrack) -> Dict[int, float]:
        similarity_scores = {}

        for i, track in enumerate(track_list):
            score = MusixmatchSearchResultsExtractor._create_single_track_similarity_score(
                missing_track=missing_track,
                track=track
            )
            similarity_scores[i] = score

        return similarity_scores

    @staticmethod
    def _create_single_track_similarity_score(missing_track: MissingTrack, track: dict) -> float:
        current_track_name = MusixmatchSearchResultsExtractor._extract_single_track_field(track, TRACK_NAME)
        current_artist_name = MusixmatchSearchResultsExtractor._extract_single_track_field(track, ARTIST_NAME)
        raw_similarities = [
            compute_similarity_score(missing_track.track_name, current_track_name),
            compute_similarity_score(missing_track.artist_name, current_artist_name),
        ]

        return mean(raw_similarities)

    @staticmethod
    def _extract_single_track_field(track: dict, field_name: str) -> Union[str, int]:
        return safe_nested_get(track, [TRACK, field_name], default="")
