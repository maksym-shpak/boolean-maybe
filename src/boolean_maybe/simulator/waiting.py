"""Injectable timeout waiting for the 11-second timeout-style presets.

Production code waits a real, fixed 11 seconds (longer than the ADR-006
ten-second client deadline) while remaining interruptible by clean shutdown.
Tests inject a fake `Waiter` instead of sleeping for the real duration.
"""

from __future__ import annotations

import threading


class Waiter:
    """Waits out the fixed timeout duration, or returns early on shutdown."""

    def wait(self, shutdown_event: threading.Event) -> bool:
        """Return True if interrupted by shutdown before the duration elapsed."""

        raise NotImplementedError


class RealWaiter(Waiter):
    """Waits the real fixed timeout duration used by timeout-style presets."""

    DURATION_SECONDS = 11.0

    def wait(self, shutdown_event: threading.Event) -> bool:
        return shutdown_event.wait(self.DURATION_SECONDS)
