from typing import Tuple

from PIL import ImageFont
from pydantic.main import BaseModel


class Font(BaseModel):
    path: str
    size: int

    def to_image_font(self) -> ImageFont:
        return ImageFont.truetype(self.path, self.size)


class ImageText(BaseModel):
    font: Font
    text: str
    position: Tuple[int, int]
    color: str
    bidi: bool = True
