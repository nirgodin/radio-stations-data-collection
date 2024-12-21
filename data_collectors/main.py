import os
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import Annotated, Dict

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException
from fastapi.params import Depends
from genie_common.tools import logger
from starlette.responses import JSONResponse

from data_collectors.app.jobs_loader import JobsLoader
from data_collectors.components import ComponentFactory
from data_collectors.logic.models import ScheduledJob
from data_collectors.scheduler_builder import SchedulerBuilder


@lru_cache
def get_component_factory() -> ComponentFactory:
    return ComponentFactory()


@asynccontextmanager
async def lifespan(app: FastAPI) -> None:
    scheduler = AsyncIOScheduler()

    try:
        scheduler_builder = SchedulerBuilder(get_component_factory())
        await scheduler_builder.build(scheduler)
        logger.info("Starting scheduler")
        scheduler.start()

        yield

    except Exception as e:
        scheduler.shutdown()
        raise


async def get_jobs_map() -> Dict[str, ScheduledJob]:
    component_factory = get_component_factory()
    return await JobsLoader.load(component_factory, "jobs")


app = FastAPI(lifespan=lifespan)


@app.get("/run/{job_id}")
async def run(job_id: str, jobs_map: Annotated[Dict[str, ScheduledJob], Depends(get_jobs_map)]):
    job = jobs_map.get(job_id)

    if job is None:
        raise HTTPException(status_code=400, detail=f"Job {job_id} not found")

    await job.task()
    return JSONResponse({"status": "ok"})


if __name__ == "__main__":
    import uvicorn as uvicorn
    port = int(os.getenv("PORT", 8080))
    host = os.getenv("HOST", "0.0.0.0")
    uvicorn.run("main:app", host=host, port=port, reload=True)
