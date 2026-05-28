from __future__ import annotations

import asyncio
import time
from collections import deque
import threading

class AsyncRateLimiter:
    """Simple async sliding-window rate limiter."""

    def __init__(self, requests_per_second: float) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self.requests_per_second = requests_per_second
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until the next request can be sent."""
        async with self._lock:
            while True:
                now = time.monotonic()
                while self._timestamps and now - self._timestamps[0] >= 1.0:
                    self._timestamps.popleft()
                if len(self._timestamps) < self.requests_per_second:
                    self._timestamps.append(now)
                    return
                sleep_for = max(0.0, 1.0 - (now - self._timestamps[0]))
                await asyncio.sleep(sleep_for)

class SyncRateLimiter:
    """Thread-safe sliding-window request limiter for synchronous OpenAlex calls."""

    def __init__(self, requests_per_second: float) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self.requests_per_second = requests_per_second
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until a request slot is available."""
        while True:
            with self._lock:
                now = time.monotonic()
                while self._timestamps and now - self._timestamps[0] >= 1.0:
                    self._timestamps.popleft()
                if len(self._timestamps) < self.requests_per_second:
                    self._timestamps.append(now)
                    return
                sleep_for = max(0.0, 1.0 - (now - self._timestamps[0]))
            time.sleep(sleep_for)


__all__ = ["AsyncRateLimiter"]
