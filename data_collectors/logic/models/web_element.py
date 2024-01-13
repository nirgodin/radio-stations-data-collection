from dataclasses import dataclass
from typing import Optional

from data_collectors.logic.models.html_element import HTMLElement


@dataclass
class WebElement:
    name: str
    type: HTMLElement
    class_: Optional[str] = None
    child_element: Optional['WebElement'] = None
    multiple: bool = False
    enumerate: bool = True
