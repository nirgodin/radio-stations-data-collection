from __future__ import annotations
from dataclasses import dataclass

from random import randint

from genie_common.utils import random_alphanumeric_string

from data_collectors.consts.shazam_consts import KEY, TITLE, ADAM_ID, DATA, ATTRIBUTES
from data_collectors.consts.spotify_consts import ARTISTS, ID, NAME


@dataclass
class ShazamTrackResources:
    id: int
    artist_id: int

    @classmethod
    def random(cls) -> ShazamTrackResources:
        return cls(id=randint(1, 50000), artist_id=randint(1, 50000))

    def to_track_response(self) -> dict:
        return {
            KEY: str(self.id),
            TITLE: random_alphanumeric_string(),
            ARTISTS: [{ADAM_ID: str(self.artist_id)}],
        }

    def to_artist_response(self) -> dict:
        return {
            DATA: [
                {
                    ID: str(self.artist_id),
                    ATTRIBUTES: {NAME: random_alphanumeric_string()},
                }
            ]
        }
