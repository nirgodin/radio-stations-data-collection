from typing import List, Annotated

from fastapi import APIRouter
from fastapi.params import Depends
from starlette.responses import JSONResponse

from data_collectors.app.utils import get_jobs_map, get_component_factory
from data_collectors.components import ComponentFactory
from data_collectors.jobs.job_id import JobId

jobs_router = APIRouter(prefix="/jobs")


@jobs_router.get("/")
async def list_jobs(
    component_factory: Annotated[ComponentFactory, Depends(get_component_factory)]
) -> List[str]:
    jobs_map = await get_jobs_map(component_factory)
    return sorted(jobs_map.keys())


@jobs_router.post("/trigger/{job_id}")
async def trigger_job(
    job_id: JobId,
    component_factory: Annotated[ComponentFactory, Depends(get_component_factory)],
) -> JSONResponse:
    jobs_map = await get_jobs_map(component_factory)
    job = jobs_map[job_id.value]
    await job.task()

    return JSONResponse({"status": "ok"})
