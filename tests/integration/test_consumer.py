"""The stand-in consumer against a real Redis: XACK after handling, redelivery after a
crash, and claiming events a dead consumer left behind."""

import signal
import socket
import time
from unittest import mock

import pytest
import redis

from pipeline.consumer import DEFAULT_GROUP, RawEventConsumer
from pipeline.publisher import RedisStreamPublisher
from tests.conftest import free_port, start_consumer

pytestmark = pytest.mark.integration


def publish(client, stream, n):
    publisher = RedisStreamPublisher(client, stream)
    return [publisher.publish({"control_id": f"C{i}", "raw_key": f"k{i}"}) for i in range(n)]


def pending_count(client, stream):
    return client.xpending(stream, DEFAULT_GROUP)["pending"]


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
    redis_client.xreadgroup(DEFAULT_GROUP, "dead", {stream_name: ">"}, count=10)
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
    """A real Redis client whose first `failures` calls (only to `method`, if given) fail
    as if Redis were down."""

    def __init__(self, inner, failures, method=None):
        self.inner = inner
        self.failures = failures
        self.method = method

    def __getattr__(self, name):
        attr = getattr(self.inner, name)
        if not callable(attr) or (self.method and name != self.method):
            return attr

        def call(*args, **kwargs):
            if self.failures > 0:
                self.failures -= 1
                raise redis.ConnectionError("simulated: Redis is down")
            return attr(*args, **kwargs)

        return call


def test_consumer_waits_out_a_redis_outage_instead_of_crashing(redis_client, stream_name):
    publish(redis_client, stream_name, 3)
    seen = []
    consumer = RawEventConsumer(
        FlakyClient(redis_client, failures=3),
        stream_name,
        consumer="c1",
        handler=lambda i, f: seen.append(f["control_id"]),
    )

    # Patch out the real waiting (it would take 3.5 s) and record the delays instead.
    with mock.patch("pipeline.consumer._wait") as wait:
        consumer.run_forever(block_ms=100, should_stop=lambda: len(seen) == 3)

    waits = [c.args[0] for c in wait.call_args_list]
    assert waits == [0.5, 1.0, 2.0]  # waited longer each time Redis was unreachable
    assert seen == ["C0", "C1", "C2"]
    assert pending_count(redis_client, stream_name) == 0


def test_consumer_rereads_its_pending_events_after_reconnecting(redis_client, stream_name):
    # Redis goes away after the consumer handled an event but before its XACK. The event
    # is still pending for c1, so reading new events (">") would never return it: after
    # reconnecting, the consumer must re-read its own pending events first.
    publish(redis_client, stream_name, 1)
    seen = []
    consumer = RawEventConsumer(
        FlakyClient(redis_client, failures=1, method="xack"),
        stream_name,
        consumer="c1",
        handler=lambda i, f: seen.append(f["control_id"]),
    )
    give_up = time.monotonic() + 3  # without the re-read, the second delivery never comes
    with mock.patch("pipeline.consumer._wait"):  # no real waiting between reconnects
        consumer.run_forever(block_ms=100, should_stop=lambda: len(seen) == 2 or time.monotonic() > give_up)

    assert seen == ["C0", "C0"]  # handled again after reconnecting: at least once
    assert pending_count(redis_client, stream_name) == 0


# --- shutdown of the real consumer process (what `docker stop` and Ctrl-C do) --------


def wait_for(condition, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "timed out waiting"
        time.sleep(0.05)


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT], ids=["sigterm", "sigint"])
def test_consumer_process_stops_cleanly_on_signal(tmp_path, redis_url, redis_client, stream_name, sig):
    log_path = tmp_path / "consumer.log"
    consumer = start_consumer(redis_url, stream_name, log_path)
    try:
        publish(redis_client, stream_name, 1)
        wait_for(lambda: redis_client.exists(stream_name) and _entries_read(redis_client, stream_name) == 1)

        consumer.send_signal(sig)
        assert consumer.wait(timeout=3) == 0  # stops within one read wait (1 s), cleanly
    finally:
        consumer.kill()

    assert "stopped" in log_path.read_text()
    assert pending_count(redis_client, stream_name) == 0  # nothing left half-handled


def test_consumer_process_stops_promptly_while_waiting_for_redis(tmp_path):
    # Redis unreachable: the consumer is waiting between reconnect attempts (delays grow
    # to 10 s). A stop request must still be noticed right away, not after the delay.
    log_path = tmp_path / "consumer.log"
    consumer = start_consumer(f"redis://127.0.0.1:{free_port()}/0", "hl7.raw", log_path)
    try:
        wait_for(lambda: "retrying in 4.0s" in log_path.read_text())  # well into the waits

        started = time.monotonic()
        consumer.send_signal(signal.SIGTERM)
        assert consumer.wait(timeout=3) == 0
        assert time.monotonic() - started < 1.0  # not the remaining ~4 s
    finally:
        consumer.kill()

    assert "stopped" in log_path.read_text()


def test_consumer_process_stops_while_redis_is_frozen(tmp_path):
    # A Redis that accepts connections but never answers (frozen, or a network black
    # hole). Without a socket timeout, the consumer would wait for a reply forever and
    # never notice the stop request; with one, the call fails and the loop exits.
    log_path = tmp_path / "consumer.log"
    with socket.create_server(("127.0.0.1", 0)) as frozen:
        frozen.settimeout(10)
        consumer = start_consumer(f"redis://127.0.0.1:{frozen.getsockname()[1]}/0", "hl7.raw", log_path)
        try:
            connection, _ = frozen.accept()
            with connection:
                connection.recv(1024)  # its first command: now it waits for a reply
                consumer.send_signal(signal.SIGTERM)
                assert consumer.wait(timeout=10) == 0  # within the 5 s socket timeout
        finally:
            consumer.kill()

    assert "stopped" in log_path.read_text()


def _entries_read(client, stream):
    """How many events the consumer group has been handed so far (0 if no group yet)."""
    groups = client.xinfo_groups(stream)
    return groups[0]["entries-read"] if groups else 0
