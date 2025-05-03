from typing import Optional

from pydantic import BaseModel


class LocalizedEntity(BaseModel):
    name: str
    translation: Optional[str] = None
