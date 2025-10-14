from datetime import datetime
from html import unescape
from typing import List, Dict, Any, Optional, Generator

from bs4 import BeautifulSoup
from genie_common.tools import AioPoolExecutor
from genie_common.utils import chain_lists
from genie_common.utils import safe_nested_get
from genie_datastores.models import DataSource
from genie_datastores.postgres.models import CuratorCollection
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import ICollector
from data_collectors.logic.models import Curation
from data_collectors.tools.josie_client import JosieClient


class JosieCurationsCollector(ICollector):
    def __init__(
        self, josie_client: JosieClient, db_engine: AsyncEngine, pool_executor: AioPoolExecutor = AioPoolExecutor()
    ):
        self._josie_client = josie_client
        self._db_engine = db_engine
        self._pool_executor = pool_executor

    async def collect(self) -> List[Curation]:
        curators = await self._josie_client.get_curators()
        curations = await self._pool_executor.run(
            iterable=curators,
            func=self._fetch_new_curator_collections,
            expected_type=list,
        )

        return chain_lists(curations)

    async def _fetch_new_curator_collections(self, curator: Dict[str, Any]) -> List[Curation]:
        posts = await self._josie_client.get_curator_posts(curator["clerkId"])
        curations = []

        for post in posts:
            post_curations = await self._fetch_single_post_curations(post)
            curations.extend(post_curations)

        return curations

    async def _fetch_single_post_curations(self, post: Dict[str, Any]) -> List[Curation]:
        is_new_post = await self._is_new_post(post["id"])

        if is_new_post:
            tracks_ids = self._extract_post_tracks_ids(post)
            return list(self._to_curations(post, tracks_ids))

        return []

    async def _is_new_post(self, post_id: str) -> bool:
        query = select(CuratorCollection.id).where(CuratorCollection.id == post_id).limit(1)
        cursor = await execute_query(engine=self._db_engine, query=query)
        query_result = cursor.scalars().first()

        return True if query_result is None else False

    def _extract_post_tracks_ids(self, post: Dict[str, Any]) -> List[str]:
        paragraphs = post["paragraphs"]

        for paragraph in paragraphs:
            if self._is_spotify_tracks_paragraph(paragraph):
                return self._extract_paragraph_tracks_ids(paragraph)

        return []

    @staticmethod
    def _is_spotify_tracks_paragraph(paragraph: Dict[str, Any]) -> bool:
        if paragraph["type"] == "audio":
            return safe_nested_get(paragraph, ["content", "provider"]) == "Spotify"

        return False

    @staticmethod
    def _extract_paragraph_tracks_ids(paragraph: Dict[str, Any]) -> List[str]:
        tracks = safe_nested_get(paragraph, ["content", "tracks"])
        tracks_ids = filter(lambda x: isinstance(x, str), [track.get("id") for track in tracks])

        return list(tracks_ids)

    def _to_curations(self, post: Dict[str, Any], tracks_ids: List[str]) -> Generator[Curation, None, None]:
        curator_id = safe_nested_get(post, ["author", "id"])
        collection_id = post["id"]
        curator_name = safe_nested_get(post, ["author", "fullName"])
        collection_date = datetime.strptime(post["publishedAt"], "%Y-%m-%d %H:%M:%S")
        collection_name = post["title"]
        collection_description = self._extract_collection_description(post)

        for track_id in tracks_ids:
            yield Curation(
                collection_id=collection_id,
                curator_id=curator_id,
                curator_name=curator_name,
                date=collection_date,
                description=collection_description,
                source=DataSource.JOSIE,
                title=collection_name,
                track_id=track_id,
            )

    def _extract_collection_description(self, post: Dict[str, Any]) -> Optional[str]:
        for paragraph in post["paragraphs"]:
            if self._is_description_paragraph(paragraph):
                html = safe_nested_get(paragraph, ["content", "text"])
                soup = BeautifulSoup(unescape(html), "html.parser")

                return soup.text

    @staticmethod
    def _is_description_paragraph(paragraph: Dict[str, Any]) -> bool:
        return paragraph["type"] == "text"
