from dataclasses import dataclass


@dataclass
class GoogleSearchConfig:
    base_url: str
    api_key: str
    cx: str
