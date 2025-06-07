from dataclasses import dataclass
from typing import List


@dataclass
class MockGenerateContentResponse:
    parts: List[str]
    text: str
