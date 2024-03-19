from typing import List, Optional, Dict, Union

from bs4 import BeautifulSoup, Tag

from data_collectors.logic.models import WebElement, HTMLElement


class WebElementsExtractor:
    def extract(self, html: str, web_element: WebElement) -> List[Optional[Dict[str, str]]]:
        soup = BeautifulSoup(html, "html.parser")
        tag = soup.find(web_element.type.value, class_=web_element.class_)

        if web_element.multiple:
            details = self._extract_multiple_details(soup, web_element)

        elif web_element.child_element is None:
            details = self._extract_single_detail(tag, web_element)

        elif web_element.child_element.multiple:
            details = self._extract_multiple_details(tag, web_element.child_element)

        else:
            details = self._extract_single_detail(tag, web_element.child_element)

        return self._serialize_details(details)

    def _extract_multiple_details(self, tag: Optional[Tag], web_element: WebElement) -> List[Optional[Dict[str, str]]]:
        if tag is None:
            return []

        tags = tag.find_all(web_element.type.value, class_=web_element.class_)
        if web_element.child_element is not None and tags is not None:
            tags = [self._extract_child_elements_tags(tag, web_element.child_element) for tag in tags]

        if tags is None:
            return []

        if web_element.enumerate:
            return [self._extract_single_detail(child_tag, web_element, i + 1) for i, child_tag in enumerate(tags)]
        else:
            return [self._extract_single_detail(child_tag, web_element) for child_tag in tags]

    def _extract_child_elements_tags(self, father_element_tag: Tag, child_element: WebElement) -> Tag:
        child_tag = father_element_tag.find_next(child_element.type.value, class_=child_element.class_)

        if child_element.child_element is None:
            return child_tag
        else:
            return self._extract_child_elements_tags(
                father_element_tag=child_tag,
                child_element=child_element.child_element
            )

    def _extract_single_detail(self, tag: Tag, web_element: WebElement, number: Optional[int] = None) -> Optional[List[Dict[str, str]]]:
        if tag is None:
            return

        tag_value = self._extract_tag_value(tag, web_element)

        if isinstance(tag_value, list):
            return [{web_element.name: line} for line in tag_value]

        if web_element.type == HTMLElement.A:
            return [{tag_value: tag["href"]}]

        if number is None:
            return [{web_element.name: tag_value}]

        return [{f"{web_element.name}{number}": tag_value}]

    def _extract_tag_value(self, tag: Tag, web_element: WebElement) -> Union[str, List[str]]:
        if web_element.expected_type == str:
            if web_element.split_breaks:
                return self._split_line_breaks(tag)

            return tag.text

        if web_element.expected_type == list:
            return list(tag.stripped_strings)

        raise ValueError(f"Got unexpected return type for web element `{web_element.name}`")

    @staticmethod
    def _split_line_breaks(tag: Tag) -> List[str]:
        split_text = []
        lines = tag.find_all(HTMLElement.BR.value)

        if not lines:
            return [tag.text]

        for i, line in enumerate(lines):
            if i == 0:
                split_text.append(line.previous.text)

            split_text.append(line.next.text)

        return split_text

    @staticmethod
    def _serialize_details(details: List[Union[List[Dict[str, str]], Dict[str, str]]]) -> List[Dict[str, str]]:
        serialized_details = []

        for detail in details:
            if isinstance(detail, dict):
                serialized_details.append(detail)
            elif isinstance(detail, list):
                serialized_details.extend(detail)
            else:
                raise ValueError("Encountered unexpected detail type. Only list and dict are supported")

        return serialized_details
