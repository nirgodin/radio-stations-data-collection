from functools import partial
from typing import Sized, Any, Awaitable, Callable, List, Optional, Type

from asyncio_pool import AioPool
from tqdm import tqdm

from data_collectors.logs import logger


class AioPoolExecutor:
    def __init__(self, pool_size: int = 5, validate_results: bool = False):
        self._pool_size = pool_size
        self._validate_results = validate_results

    async def run(self,
                  iterable: Sized,
                  func: Callable[..., Awaitable[Any]],
                  expected_type: Optional[Type] = None) -> List[Any]:
        pool = AioPool(self._pool_size)

        with tqdm(total=len(iterable)) as progress_bar:
            monitored_func = partial(self._execute_single, progress_bar, func)
            results = await pool.map(monitored_func, iterable)

        if self._validate_results:
            return [result for result in results if isinstance(result, expected_type)]

        return results

    @staticmethod
    async def _execute_single(progress_bar: tqdm, func: Callable[..., Awaitable[Any]], value: Any) -> Any:
        try:
            return await func(value)

        except Exception as e:
            logger.exception("PoolExecutor encountered exception")
            return e

        finally:
            progress_bar.update(1)
