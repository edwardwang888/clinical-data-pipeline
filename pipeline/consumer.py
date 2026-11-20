"""Stand-in for the parser: read `hl7.raw` with a consumer group, log each event, XACK it.

Run it:  python -m pipeline.consumer

How at-least-once delivery works with Redis Streams consumer groups:

- Each event is delivered to one consumer in the group. Redis remembers every event it
  delivered that hasn't been XACKed yet (the group's "pending entries list").
- We XACK only *after* an event is handled. If we crash in between, the event stays
  pending and is delivered again, so handling must be idempotent (a duplicate must be
  harmless). Logging is; the real parser will deduplicate on control ID and hash.
- On startup we first re-read our *own* pending events (delivered to us before a
  crash, never XACKed).
- XAUTOCLAIM takes over events another consumer received but hasn't XACKed for
  `claim_idle_ms`: that consumer has probably died.
"""

from __future__ import annotations

import logging
import os
import signal
import socket
from collections.abc import Callable

import redis

from pipeline.publisher import RAW_STREAM

log = logging.getLogger("pipeline.consumer")  # stable name even when run with -m

DEFAULT_GROUP = "raw-logger"
BATCH = 100

Handler = Callable[[str, dict[str, str]], None]


def log_event(event_id: str, fields: dict[str, str]) -> None:
    log.info(
        "event id=%s control_id=%s type=%s raw_key=%s",
        event_id,
        fields.get("control_id"),
        fields.get("message_type"),
        fields.get("raw_key"),
    )


class RawEventConsumer:
    def __init__(
        self,
        client: redis.Redis,
        stream: str = RAW_STREAM,
        group: str = DEFAULT_GROUP,
        consumer: str | None = None,
        handler: Handler = log_event,
        claim_idle_ms: int = 60_000,
    ) -> None:
        self.client = client
        self.stream = stream
        self.group = group
        self.consumer = consumer or socket.gethostname()
        self.handler = handler
        self.claim_idle_ms = claim_idle_ms

    def ensure_group(self) -> None:
        """Create the consumer group if needed. id="0" means a new group starts from
        the beginning of the stream, so events published before it existed are not
        skipped."""
        try:
            self.client.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):  # BUSYGROUP = the group already exists
                raise

    def process_own_pending(self) -> int:
        """Re-handle events delivered to this consumer earlier but never XACKed."""
        handled = 0
        # Reading from an explicit id (instead of ">") returns our pending events after
        # that id. Walk forward so an event that fails again doesn't loop forever.
        start = "0"
        while True:
            entries = self._read(start, block_ms=None)
            if not entries:
                return handled
            handled += self._handle(entries)
            start = entries[-1][0]

    def claim_stale(self) -> int:
        """Take over events another consumer has held for too long without XACKing."""
        handled = 0
        start = "0-0"
        while True:
            next_start, entries, *_ = self.client.xautoclaim(
                self.stream, self.group, self.consumer, self.claim_idle_ms, start, count=BATCH
            )
            handled += self._handle(entries)
            if next_start in ("0-0", b"0-0"):
                return handled
            start = next_start

    def process_new(self, block_ms: int | None = 5000) -> int:
        """Handle events never delivered to anyone yet (waits up to `block_ms`)."""
        return self._handle(self._read(">", block_ms))

    def run_forever(self, block_ms: int = 5000, should_stop: Callable[[], bool] = lambda: False) -> None:
        self.ensure_group()
        self.process_own_pending()
        while not should_stop():
            self.claim_stale()
            self.process_new(block_ms)

    def _read(self, start_id: str, block_ms: int | None) -> list[tuple[str, dict | None]]:
        response = self.client.xreadgroup(
            self.group, self.consumer, {self.stream: start_id}, count=BATCH, block=block_ms
        )
        if not response:
            return []
        _stream, entries = response[0]
        return entries

    def _handle(self, entries) -> int:
        handled = 0
        for event_id, fields in entries:
            if not fields:
                # The entry was deleted from the stream; nothing to do but clear it.
                self.client.xack(self.stream, self.group, event_id)
                continue
            try:
                self.handler(event_id, fields)
            except Exception:
                # No XACK: the event stays pending and will be delivered again.
                log.exception("handling failed, leaving pending for redelivery id=%s", event_id)
                continue
            self.client.xack(self.stream, self.group, event_id)
            handled += 1
        return handled


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    client = redis.Redis.from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True
    )
    consumer = RawEventConsumer(
        client,
        stream=os.environ.get("RAW_STREAM", RAW_STREAM),
        group=os.environ.get("CONSUMER_GROUP", DEFAULT_GROUP),
        consumer=os.environ.get("CONSUMER_NAME") or None,
    )

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    log.info("consuming stream=%s group=%s consumer=%s", consumer.stream, consumer.group, consumer.consumer)
    consumer.run_forever(should_stop=lambda: stopping)
    log.info("stopped")


if __name__ == "__main__":
    main()
