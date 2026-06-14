"""Lightweight retry decorator with exponential backoff.

Used to wrap network-fronted calls (Snowflake INSERT batches, COPY into stage).
Deliberately tiny - for anything more elaborate, switch to ``tenacity``.
"""
from __future__ import annotations

import functools
import logging
import random
import time
from collections.abc import Callable
from typing import TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


def retry(
    max_attempts: int = 3,
    backoff_seconds: float = 2.0,
    max_backoff_seconds: float = 60.0,
    jitter: bool = True,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator: retry a callable on transient failure with exponential backoff.

    The final attempt re-raises so callers can decide how to surface the error.

    Example::

        @retry(max_attempts=5, backoff_seconds=1.0)
        def copy_into_snowflake(...):
            ...
    """

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> T:
            attempt = 0
            wait = backoff_seconds
            while True:
                attempt += 1
                try:
                    return fn(*args, **kwargs)
                except retry_on as exc:
                    if attempt >= max_attempts:
                        logger.error(
                            "%s failed after %d attempts: %s",
                            fn.__qualname__, attempt, exc,
                        )
                        raise
                    sleep_for = min(wait, max_backoff_seconds)
                    if jitter:
                        sleep_for *= 0.5 + random.random()  # [0.5x, 1.5x)
                    logger.warning(
                        "%s attempt %d/%d failed (%s); retrying in %.1fs",
                        fn.__qualname__, attempt, max_attempts, exc, sleep_for,
                    )
                    time.sleep(sleep_for)
                    wait = min(wait * 2, max_backoff_seconds)

        return wrapper

    return decorator
