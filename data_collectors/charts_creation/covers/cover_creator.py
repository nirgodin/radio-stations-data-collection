from io import BytesIO
from typing import List

from PIL import Image, ImageDraw
from arabic_reshaper import reshape
from bidi.algorithm import get_display

from data_collectors.charts_creation.covers.image_text import ImageText


class CoverCreator:
    def __init__(self, source_path: str):
        self._source_path = source_path

    def create(self, image_texts: List[ImageText]) -> bytes:
        image = Image.open(self._source_path).convert("RGB")
        draw = ImageDraw.Draw(image)

        for image_text in image_texts:
            draw.text(
                xy=image_text.position,
                text=self._pre_process_text(image_text),
                fill=image_text.color,
                font=image_text.font.to_image_font(),
            )

        return self._convert_image_to_bytes(image)

    @staticmethod
    def _pre_process_text(image_text: ImageText) -> str:
        if not image_text.bidi:
            return image_text.text

        reshaped_text = reshape(image_text.text)
        return get_display(reshaped_text)

    @staticmethod
    def _convert_image_to_bytes(image: Image) -> bytes:
        buffer = BytesIO()
        image.save(buffer, format="JPEG")
        buffer.seek(0)

        return buffer.getvalue()
