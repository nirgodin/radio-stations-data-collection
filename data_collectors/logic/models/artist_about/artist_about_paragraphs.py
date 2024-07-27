from typing import List

from pydantic import BaseModel


class ArtistAboutParagraphs(BaseModel):
    paragraphs: List[str]
