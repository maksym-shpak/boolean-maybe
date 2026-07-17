"""Injectable async sleeping for invocation-local waits.

The application workflow is asynchronous (ADR-002), so waiting must remain
cancellable through `asyncio.CancelledError` rather than blocking a worker
thread. Tests inject a fake `Sleeper` that records requested durations and
advances a paired fake `MonotonicClock` instead of sleeping for real.
"""

from __future__ import annotations

import asyncio
from typing import Protocol


class Sleeper(Protocol):
    async def sleep(self, seconds: float) -> None: ...


class RealSleeper:
    """Sleeps the real requested duration."""

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)
