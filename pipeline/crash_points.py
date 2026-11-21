"""Test-only crash points: make the process die at one exact step.

The integration tests set CRASH_AT=<point> and CRASH_ON=<n> for a listener process, and
it exits on the spot (no cleanup, like a power cut) the n-th time it reaches that point.
That way the most dangerous moments, such as "file saved but event not yet published",
are tested on every run instead of only when a random kill happens to land there.

With CRASH_AT unset, which is always the case outside those tests, this does nothing.

Points used:
    after_save      the raw file is saved, the event is not yet published
    after_publish   the event is published, the AA is not yet sent
    mid_write       a temp file is written and fsynced, not yet linked to its final name
"""

from __future__ import annotations

import os
import threading

_hits: dict[str, int] = {}
_lock = threading.Lock()  # saves run in worker threads


def crash_point(name: str) -> None:
    if os.environ.get("CRASH_AT") != name:
        return
    with _lock:
        _hits[name] = _hits.get(name, 0) + 1
        if _hits[name] == int(os.environ.get("CRASH_ON", "1")):
            os._exit(70)  # immediately, skipping every finally block, like a real crash
