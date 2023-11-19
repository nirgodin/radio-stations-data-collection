from functools import partial
from typing import Sized, Any, Awaitable, Callable, List

from asyncio_pool import AioPool
from tqdm import tqdm

from data_collectors.logs import logger


class AioPoolExecutor:
    def __init__(self, pool_size: int = 5):
        self._pool = AioPool(pool_size)

    async def run(self, iterable: Sized, func: Callable[..., Awaitable[Any]]) -> List[Any]:
        with tqdm(total=len(iterable)) as progress_bar:
            monitored_func = partial(self._execute_single, progress_bar, func)
            return await self._pool.map(monitored_func, iterable)

    @staticmethod
    async def _execute_single(progress_bar: tqdm, func: Callable[..., Awaitable[Any]], value: Any) -> Any:
        try:
            return await func(value)

        except Exception as e:
            logger.exception("PoolExecutor encountered exception")
            return e

        finally:
            progress_bar.update(1)
