"""The stand-in consumer against a real Redis: XACK after handling, redelivery after a
crash, and claiming events a dead consumer left behind."""

import pytest
import redis

from pipeline.consumer import RawEventConsumer
from pipeline.publisher import RedisStreamPublisher

pytestmark = pytest.mark.integration


def publish(client, stream, n):
    publisher = RedisStreamPublisher(client, stream)
    return [publisher.publish({"control_id": f"C{i}", "raw_key": f"k{i}"}) for i in range(n)]


def pending_count(client, stream, group="raw-logger"):
    return client.xpending(stream, group)["pending"]


def test_handles_and_xacks_every_event(redis_client, stream_name):
    seen = []
    consumer = RawEventConsumer(
        redis_client, stream_name, consumer="c1", handler=lambda i, f: seen.append(f["control_id"])
    )
    consumer.ensure_group()
    publish(redis_client, stream_name, 5)

    assert consumer.process_new(block_ms=100) == 5
    assert seen == ["C0", "C1", "C2", "C3", "C4"]
    assert pending_count(redis_client, stream_name) == 0


def test_events_published_before_the_group_existed_are_not_skipped(redis_client, stream_name):
    publish(redis_client, stream_name, 3)
    consumer = RawEventConsumer(redis_client, stream_name, consumer="c1", handler=lambda i, f: None)
    consumer.ensure_group()
    assert consumer.process_new(block_ms=100) == 3


def test_ensure_group_is_safe_to_call_twice(redis_client, stream_name):
    consumer = RawEventConsumer(redis_client, stream_name, consumer="c1")
    consumer.ensure_group()
    consumer.ensure_group()


def test_failed_event_stays_pending_and_is_redelivered_after_restart(redis_client, stream_name):
    def crash_on_c1(event_id, fields):
        if fields["control_id"] == "C1":
            raise RuntimeError("simulated crash before XACK")

    first = RawEventConsumer(redis_client, stream_name, consumer="c1", handler=crash_on_c1)
    first.ensure_group()
    publish(redis_client, stream_name, 3)
    assert first.process_new(block_ms=100) == 2
    assert pending_count(redis_client, stream_name) == 1

    # "Restart": a new process with the same consumer name re-reads its own pending.
    seen = []
    restarted = RawEventConsumer(
        redis_client, stream_name, consumer="c1", handler=lambda i, f: seen.append(f["control_id"])
    )
    assert restarted.process_own_pending() == 1
    assert seen == ["C1"]
    assert pending_count(redis_client, stream_name) == 0


def test_own_pending_with_a_repeat_failure_does_not_loop_forever(redis_client, stream_name):
    def always_fail(event_id, fields):
        raise RuntimeError("still broken")

    consumer = RawEventConsumer(redis_client, stream_name, consumer="c1", handler=always_fail)
    consumer.ensure_group()
    publish(redis_client, stream_name, 2)
    consumer.process_new(block_ms=100)
    assert consumer.process_own_pending() == 0
    assert pending_count(redis_client, stream_name) == 2


def test_events_held_by_a_dead_consumer_are_claimed(redis_client, stream_name):
    dead = RawEventConsumer(redis_client, stream_name, consumer="dead")
    dead.ensure_group()
    publish(redis_client, stream_name, 2)
    # "dead" receives the events, then dies without XACKing them.
    redis_client.xreadgroup("raw-logger", "dead", {stream_name: ">"}, count=10)
    assert pending_count(redis_client, stream_name) == 2

    seen = []
    survivor = RawEventConsumer(
        redis_client,
        stream_name,
        consumer="alive",
        handler=lambda i, f: seen.append(f["control_id"]),
        claim_idle_ms=0,  # in production: 60 s, so a slow-but-alive consumer keeps its work
    )
    assert survivor.claim_stale() == 2
    assert seen == ["C0", "C1"]
    assert pending_count(redis_client, stream_name) == 0


class FlakyClient:
    """A real Redis client whose first `failures` calls fail as if Redis were down."""

    def __init__(self, inner, failures):
        self.inner = inner
        self.failures = failures

    def __getattr__(self, name):
        attr = getattr(self.inner, name)
        if not callable(attr):
            return attr

        def call(*args, **kwargs):
            if self.failures > 0:
                self.failures -= 1
                raise redis.ConnectionError("simulated: Redis is down")
            return attr(*args, **kwargs)

        return call


def test_consumer_waits_out_a_redis_outage_instead_of_crashing(redis_client, stream_name):
    publish(redis_client, stream_name, 3)
    seen, waits = [], []
    consumer = RawEventConsumer(
        FlakyClient(redis_client, failures=3),
        stream_name,
        consumer="c1",
        handler=lambda i, f: seen.append(f["control_id"]),
    )

    consumer.run_forever(block_ms=100, should_stop=lambda: len(seen) == 3, sleep=waits.append)

    assert waits == [0.5, 1.0, 2.0]  # waited longer each time Redis was unreachable
    assert seen == ["C0", "C1", "C2"]
    assert pending_count(redis_client, stream_name) == 0


def test_consumer_rereads_its_pending_events_after_reconnecting(redis_client, stream_name):
    # c1 received an event but never XACKed it (say Redis went down mid-handling).
    RawEventConsumer(redis_client, stream_name, consumer="c1").ensure_group()
    publish(redis_client, stream_name, 1)
    redis_client.xreadgroup("raw-logger", "c1", {stream_name: ">"}, count=10)

    seen = []
    consumer = RawEventConsumer(
        FlakyClient(redis_client, failures=1),
        stream_name,
        consumer="c1",
        handler=lambda i, f: seen.append(f["control_id"]),
    )
    consumer.run_forever(block_ms=100, should_stop=lambda: len(seen) == 1, sleep=lambda s: None)

    assert seen == ["C0"]
    assert pending_count(redis_client, stream_name) == 0
