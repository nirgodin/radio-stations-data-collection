from asyncio import new_event_loop

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from genie_common.tools import logger

from data_collectors.components import ComponentFactory
from data_collectors.scheduler_builder import SchedulerBuilder


async def run_scheduler(scheduler: AsyncIOScheduler = AsyncIOScheduler(),
                        component_factory: ComponentFactory = ComponentFactory()) -> None:
    scheduler_builder = SchedulerBuilder(component_factory)
    scheduler = await scheduler_builder.build(scheduler)

    try:
        logger.info("Starting scheduler")
        scheduler.start()
    except Exception as e:
        scheduler.shutdown()
        raise


def run():
    loop = new_event_loop()
    loop.create_task(run_scheduler())

    try:
        loop.run_forever()
    except Exception as e:
        if loop.is_running():
            loop.stop()

        loop.close()


if __name__ == "__main__":
    run()
