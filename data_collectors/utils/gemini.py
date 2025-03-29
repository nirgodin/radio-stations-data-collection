from typing import Type, Optional

from genie_common.tools import logger
from google.generativeai.types.generation_types import BaseGenerateContentResponse
from pydantic import BaseModel, ValidationError


def serialize_generative_model_response(
    response: BaseGenerateContentResponse, model: Type[BaseModel]
) -> Optional[BaseModel]:
    try:
        if response.parts:
            return model.parse_raw(response.text)

        logger.warning("Did not receive valid response parts. Returning None by default")
        return None

    except ValidationError:
        logger.exception("Was not able to serialize model response. Returning None by default")
        return None
