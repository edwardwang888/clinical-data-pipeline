"""The real listener, made to crash at one exact step. For tests only.

The restart test runs `python -m tests.crashing_listener` instead of
`python -m services.listener`, with CRASH_AT=<point> and CRASH_ON=<n>. Before starting the
normal listener, this patches one function (unittest.mock) so the process exits on the
spot the n-th time it reaches that point: no cleanup, like a power cut. That way the most
dangerous moments are tested on every run, not only when a random kill lands there.

Production code has no crash hooks at all: nothing outside this file reads CRASH_AT.

    after_save      the raw file is saved, the event is not yet published
    after_publish   the event is published, the AA is not yet sent
    mid_write       a temp file is written and fsynced, not yet linked to its final name

If the code these patches target is renamed, the crash simply never happens and the
restart test fails with a timeout (it waits for this process to exit), not silently.
"""

import os
import threading
from pathlib import Path
from unittest import mock

from pipeline.publisher import RAW_STREAM, RedisStreamPublisher
from pipeline.raw_store import LocalRawStore
from services.listener.__main__ import main

CRASH_AT = os.environ["CRASH_AT"]
CRASH_ON = int(os.environ.get("CRASH_ON", "1"))
RAW_DIR = Path(os.environ["RAW_DIR"])
STREAM = os.environ.get("RAW_STREAM", RAW_STREAM)

_hits = 0
_lock = threading.Lock()  # saves run in worker threads


def count_and_maybe_crash():
    global _hits
    with _lock:
        _hits += 1
        if _hits == CRASH_ON:
            os._exit(70)  # immediately, skipping every finally block


original_save_raw = LocalRawStore.save_raw
original_publish = RedisStreamPublisher.publish
original_link = os.link


def save_raw_then_crash(self, raw, meta):
    saved = original_save_raw(self, raw, meta)
    if self.root == RAW_DIR:  # the raw archive, not the rejected-evidence store
        count_and_maybe_crash()
    return saved


def publish_then_crash(self, event):
    event_id = original_publish(self, event)
    if self.stream == STREAM:  # hl7.raw, not the dead-letter stream
        count_and_maybe_crash()
    return event_id


def crash_before_link(src, dst, *args, **kwargs):
    count_and_maybe_crash()
    return original_link(src, dst, *args, **kwargs)


PATCHES = {
    "after_save": mock.patch.object(LocalRawStore, "save_raw", save_raw_then_crash),
    "after_publish": mock.patch.object(RedisStreamPublisher, "publish", publish_then_crash),
    "mid_write": mock.patch.object(os, "link", crash_before_link),
}

if __name__ == "__main__":
    with PATCHES[CRASH_AT]:
        main()
