from string import punctuation, whitespace
from textwrap import dedent
from typing import List, Optional

from google.generativeai import GenerativeModel

from data_collectors.contract import ISerializer
from data_collectors.logic.models import ArtistAboutParagraphs
from data_collectors.utils.gemini import serialize_generative_model_response

SHAZAM_WRITER_NAME_SEPARATOR = "~"


class ArtistsAboutParagraphsSerializer(ISerializer):
    def __init__(
        self,
        generative_model: GenerativeModel,
        min_paragraph_tokens: int = 5,
        max_paragraph_token: int = 200,
        tokens_check_number: int = 5,
    ):
        self._generative_model = generative_model
        self._min_paragraph_tokens = min_paragraph_tokens
        self._max_paragraph_token = max_paragraph_token
        self._tokens_check_number = tokens_check_number

    def serialize(self, text: str) -> List[str]:
        paragraphs = []
        paragraphs_on_hold = []

        for paragraph in text.split("\n"):
            if not self._is_relevant_paragraph(paragraph):
                continue

            formatted_paragraph = self._format_paragraph(paragraph)

            if self._is_paragraph_too_long(formatted_paragraph):
                split_paragraphs = self._split_long_paragraph(formatted_paragraph)
                current_paragraphs = self._join_current_paragraphs(
                    paragraphs_on_hold, split_paragraphs
                )
                paragraphs.extend(current_paragraphs)
                paragraphs_on_hold = []

            elif self._is_standalone_paragraph(formatted_paragraph):
                current_paragraphs = self._join_current_paragraphs(
                    paragraphs_on_hold, [formatted_paragraph]
                )
                paragraphs.extend(current_paragraphs)
                paragraphs_on_hold = []

            else:
                paragraphs_on_hold.append(formatted_paragraph)

        if paragraphs_on_hold:
            last_paragraph = "\n".join(paragraphs_on_hold)

            if self._is_standalone_paragraph(last_paragraph):
                paragraphs.append(last_paragraph)

        return paragraphs

    def _is_relevant_paragraph(self, text: str) -> bool:
        if text.strip():
            return not self._contains_only_punctuation(text)

        return False

    @staticmethod
    def _contains_only_punctuation(text: str) -> bool:  # TODO: Extract to genie-common
        return all(letter in (punctuation + whitespace) for letter in text)

    def _format_paragraph(self, text: str) -> str:
        tokens = [token.strip() for token in text.split()]
        last_token_number = self._determine_last_token_number(tokens)
        relevant_tokens = tokens[:last_token_number]

        return " ".join(relevant_tokens)

    def _determine_last_token_number(self, tokens: List[str]) -> int:
        total_tokens_number = len(tokens)
        start_token_number = max(total_tokens_number - self._tokens_check_number, 0)

        for i in range(start_token_number, total_tokens_number):
            token = tokens[i]

            if token.strip() == SHAZAM_WRITER_NAME_SEPARATOR:
                return i

        return total_tokens_number

    def _is_paragraph_too_long(self, text: str) -> bool:
        tokens_number = self._count_paragraph_tokens(text)
        return tokens_number >= self._max_paragraph_token

    def _split_long_paragraph(self, text: str) -> List[str]:
        prompt = """\
            Please return JSON splitting this following text to multiple paragraphs, using the following schema:

            {
                "paragraphs": List[str],
            }

            Each paragraph should encapsulate a single logical theme, so it could later be embedded to represent a \
            very specific context. On the other hand, make sure not to split text into too small paragraphs - a \
            paragraph should usually consist of multiple sentences. You may return any number of paragraphs you find \
            suitable, but try not to return more than 10 paragraphs if possible.

        """
        response = self._generative_model.generate_content(
            contents=dedent(prompt) + text,
            generation_config={"response_mime_type": "application/json"},
        )
        serialized_response: Optional[ArtistAboutParagraphs] = (
            serialize_generative_model_response(
                response=response, model=ArtistAboutParagraphs
            )
        )

        return [] if serialized_response is None else serialized_response.paragraphs

    def _is_standalone_paragraph(self, text: str) -> bool:
        tokens_number = self._count_paragraph_tokens(text)
        return tokens_number >= self._min_paragraph_tokens

    @staticmethod
    def _join_current_paragraphs(
        paragraphs_on_hold: List[str], current_paragraphs: List[str]
    ) -> List[str]:
        first_current_paragraph = current_paragraphs[0]
        new_first_paragraph = paragraphs_on_hold + [first_current_paragraph]

        return new_first_paragraph + current_paragraphs[1:]

    @staticmethod
    def _count_paragraph_tokens(text: str) -> int:
        return len(text.split())
