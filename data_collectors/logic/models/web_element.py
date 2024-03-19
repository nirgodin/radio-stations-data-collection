from dataclasses import dataclass
from re import Pattern
from typing import Optional, Type, Union

from data_collectors.logic.models.html_element import HTMLElement


@dataclass
class WebElement:
    name: str
    type: HTMLElement
    class_: Optional[Union[str, Pattern]] = None
    child_element: Optional['WebElement'] = None
    multiple: bool = False
    enumerate: bool = True
    expected_type: Type = str
    split_breaks: bool = False
