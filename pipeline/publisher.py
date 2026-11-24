"""Queue publishing: tell downstream consumers "a new raw message was stored".

`Publisher` is the interface the listener depends on. `RedisStreamPublisher` appends
to a Redis Stream; a Pub/Sub publisher can implement the same method later.

An event carries a *pointer* to the raw file (its key and hash), never the message
itself: the raw archive is the source of truth, and events can always be rebuilt
from it.
"""

from __future__ import annotations

from typing import Any, Protocol

import redis

# Stream that the listener publishes "new raw message" events to.
RAW_STREAM = "hl7.raw"
# Dead-letter queue: one entry per bad input (stage=listener now; later stages may add
# entries for messages they keep failing on). Each points at its evidence file.
DLQ_STREAM = "hl7.dlq"


class Publisher(Protocol):
    def publish(self, event: dict[str, Any]) -> str:
        """Publish `event` and return its id. Raise if it may not have been published."""
        ...


class RedisStreamPublisher:
    """Publisher that XADDs each event to a Redis Stream."""

    def __init__(self, client: redis.Redis, stream: str = RAW_STREAM) -> None:
        self.client = client
        self.stream = stream

    @classmethod
    def from_url(cls, url: str, stream: str = RAW_STREAM) -> RedisStreamPublisher:
        # Timeouts matter: a hung Redis must turn into an error (and an AE to the
        # sender), not a listener that waits forever while holding the message.
        client = redis.Redis.from_url(
            url, decode_responses=True, socket_timeout=5, socket_connect_timeout=5
        )
        return cls(client, stream)

    def publish(self, event: dict[str, Any]) -> str:
        # Stream fields must be strings; None becomes "".
        fields = {k: "" if v is None else str(v) for k, v in event.items()}
        return self.client.xadd(self.stream, fields)
