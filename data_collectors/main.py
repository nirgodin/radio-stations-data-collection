from asyncio import new_event_loop

from genie_common.tools import logger

from data_collectors.scheduler_builder import SchedulerBuilder


async def run_scheduler() -> None:
    scheduler_builder = SchedulerBuilder()
    scheduler = await scheduler_builder.build()

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
