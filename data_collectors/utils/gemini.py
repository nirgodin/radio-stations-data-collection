import os
from typing import Type, Optional

from genie_common.tools import logger
from google.generativeai.types.generation_types import BaseGenerateContentResponse
from pydantic import BaseModel, ValidationError

from data_collectors.utils.os import get_root_dir_path


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


def load_prompt(file_name: str) -> str:
    parent_path = get_root_dir_path()
    prompt_path = os.path.join(parent_path, "logic", "prompts", file_name)

    with open(prompt_path, "r") as f:
        return f.read()
