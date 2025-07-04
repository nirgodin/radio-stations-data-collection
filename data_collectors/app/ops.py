from typing import Annotated

from fastapi import APIRouter, Depends
from starlette.responses import JSONResponse

from data_collectors.app.utils import get_component_factory
from data_collectors.components import ComponentFactory

ops_router = APIRouter(prefix="/ops", tags=["Operations"])


@ops_router.post("/chartsMistakes")
async def fix_charts_mistakes(
    component_factory: Annotated[ComponentFactory, Depends(get_component_factory)],
) -> JSONResponse:
    async with component_factory.sessions.enter_spotify_session() as session:
        manager = component_factory.charts.get_tagged_mistakes_manager(session)
        await manager.run()

    return JSONResponse({"status": "ok"})
