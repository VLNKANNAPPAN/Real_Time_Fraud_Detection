"""Failure classification and bounded operations used by the stream processor."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass
from random import Random


class RetryableProcessingError(RuntimeError):
    """The source offset must remain uncommitted and the partition should retry."""


class RetryableDatabaseError(RetryableProcessingError):
    pass


class RetryableRedisError(RetryableProcessingError):
    pass


class RetryableFeatureError(RetryableProcessingError):
    pass


class RetryableKafkaError(RetryableProcessingError):
    pass


class NonRetryableIdentityConflict(ValueError):
    """An immutable event or transaction identity was reused with different content."""


class InferenceTimedOut(TimeoutError):
    pass


@dataclass(frozen=True)
class RetryPolicy:
    initial_seconds: float = 0.25
    maximum_seconds: float = 10.0
    jitter_fraction: float = 0.20

    def delay(self, attempt: int, *, random: Random | None = None) -> float:
        if attempt < 1:
            raise ValueError("attempt must be at least one")
        base = min(self.maximum_seconds, self.initial_seconds * (2 ** (attempt - 1)))
        rng = random or Random()
        return max(0.0, base * (1 + rng.uniform(-self.jitter_fraction, self.jitter_fraction)))


def call_with_timeout[T](operation: Callable[[], T], timeout_seconds: float) -> T:
    """Return within the deadline and ignore any late worker result."""
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="fraud-inference")
    future = executor.submit(operation)
    try:
        return future.result(timeout=timeout_seconds)
    except FutureTimeout as exc:
        future.cancel()
        raise InferenceTimedOut(f"inference exceeded {timeout_seconds:.3f}s") from exc
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
