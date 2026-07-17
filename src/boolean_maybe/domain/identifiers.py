"""Injectable local identifier generation.

`job_id`, `attempt_id`, and the internal invocation token all use independent
cryptographically secure UUID version 4 values serialized as lowercase
canonical strings. Generation is injectable so tests can inject deterministic
or colliding sequences without patching the standard library.
"""

from __future__ import annotations

import uuid
from typing import Protocol


class IdGenerator(Protocol):
    def __call__(self) -> str: ...


def generate_id() -> str:
    """Return a new lowercase canonical UUID version 4 string."""

    return str(uuid.uuid4())
