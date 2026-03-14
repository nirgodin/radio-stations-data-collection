from textwrap import dedent
from time import sleep
from typing import List, Optional

from genie_common.tools import logger, SyncPoolExecutor
from genie_common.utils import chain_lists
from genie_datastores.mongo.models import LyricsSectionDocument
from google.generativeai import GenerativeModel

from data_collectors.contract import ICollector
from data_collectors.logic.models import RawLyrics, FormattedLyrics
from data_collectors.utils.gemini import load_prompt, serialize_generative_model_response


class TracksLyricsFormatterCollector(ICollector):
    def __init__(
        self,
        generative_model: GenerativeModel,
        pool_executor: SyncPoolExecutor = SyncPoolExecutor(),
    ):
        self._generative_model = generative_model
        self._pool_executor = pool_executor

    async def collect(
        self,
        raw_lyrics: List[RawLyrics],
    ) -> List[LyricsSectionDocument]:
        results = self._pool_executor.run(
            iterable=raw_lyrics, func=self._serialize_single_track_lyrics, expected_type=list
        )
        return chain_lists(results)

    def _serialize_single_track_lyrics(
        self,
        lyrics: RawLyrics,
    ) -> Optional[List[LyricsSectionDocument]]:
        logger.info(f"Formatting track lyrics for id `{lyrics.id}`")
        prompt_format = load_prompt("tracks_lyrics_prompt_format.txt")
        prompt = prompt_format.format(track_lyrics=lyrics.lyrics)
        response = self._generative_model.generate_content(
            contents=dedent(prompt),
            generation_config={"response_mime_type": "application/json"},
        )
        sleep(30)
        formatted_lyrics: Optional[FormattedLyrics] = serialize_generative_model_response(
            response=response, model=FormattedLyrics
        )

        if isinstance(formatted_lyrics, FormattedLyrics):
            return formatted_lyrics.to_documents(
                entity_id=lyrics.id,
                source=lyrics.source,
            )
