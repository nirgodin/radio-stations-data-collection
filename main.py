import os
from contextlib import asynccontextmanager
from typing import Optional, List

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from genie_common.tools import logger

from data_collectors.app.jobs import jobs_router
from data_collectors.app.utils import get_component_factory
from data_collectors.components import ComponentFactory
from data_collectors.logic.models import ScheduledJob
from data_collectors.scheduler_builder import SchedulerBuilder


@asynccontextmanager
async def lifespan(
    app: FastAPI,
    component_factory: Optional[ComponentFactory] = None,
    jobs: Optional[List[ScheduledJob]] = None,
) -> None:
    scheduler = AsyncIOScheduler()

    try:
        scheduler_builder = SchedulerBuilder(component_factory or get_component_factory())
        await scheduler_builder.build(scheduler, jobs)
        await component_factory.tools.initialize_mongo()
        logger.info("Starting scheduler")
        scheduler.start()

        yield

    except Exception:
        scheduler.shutdown()
        raise


app = FastAPI(lifespan=lifespan)
app.include_router(jobs_router)


if __name__ == "__main__":
    import uvicorn as uvicorn

    port = int(os.getenv("PORT", 8080))
    host = os.getenv("HOST", "0.0.0.0")
    uvicorn.run("main:app", host=host, port=port, reload=True)
