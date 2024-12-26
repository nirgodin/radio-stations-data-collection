from typing import List, Annotated, Dict

from fastapi import APIRouter
from fastapi.params import Depends
from starlette.responses import JSONResponse

from data_collectors.app.utils import get_jobs_map
from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ScheduledJob

jobs_router = APIRouter(prefix="/jobs")


@jobs_router.get("/")
async def list_jobs(jobs_map: Annotated[Dict[str, ScheduledJob], Depends(get_jobs_map)]) -> List[str]:
    return sorted(jobs_map.keys())


@jobs_router.post("/trigger/{job_id}")
async def trigger_job(job_id: JobId, jobs_map: Annotated[Dict[str, ScheduledJob], Depends(get_jobs_map)]) -> JSONResponse:
    job = jobs_map[job_id.value]
    await job.task()

    return JSONResponse({"status": "ok"})
