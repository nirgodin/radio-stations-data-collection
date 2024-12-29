import asyncio
from typing import Callable, Union, Awaitable, Type, Tuple


async def until(
    condition: Callable[[], Union[bool, Awaitable[bool]]],
    timeout: float = 10,
    interval: float = 0.1,
    ignore_exception_types: Tuple[Type[Exception]] = (),
) -> None:
    start_time = asyncio.get_event_loop().time()

    async def evaluate_condition() -> bool:
        try:
            result = condition()
            if isinstance(result, Awaitable):
                result = await result

            return result
        except ignore_exception_types:
            return False

    while asyncio.get_event_loop().time() < start_time + timeout:
        if await evaluate_condition() is True:
            return

        await asyncio.sleep(interval)

    raise TimeoutError(f"Condition not met within {timeout} seconds")
