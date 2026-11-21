"""The listener's core promise: AA is sent only after the save AND the publish succeed.

These tests run the real listener on a local TCP port, with a raw store and publisher
that can be paused or made to fail, and watch what the sender receives.
"""

import asyncio
import hashlib
import threading

import pytest
from hl7.mllp import start_hl7_server

from pipeline.raw_store import LocalRawStore
from services.listener.framing import read_frame
from services.listener.server import Listener

MSG = b"MSH|^~\\&|SIMHOSP|SYNTH_GENERAL|CDP|CDP|20261120225047||ADT^A01|CTRL0001|P|2.5.1\rPID|||SYN000001\r"
MSG2 = MSG.replace(b"CTRL0001", b"CTRL0002")


class GatedStore:
    """Real LocalRawStore, but each save can be paused (gate) or made to fail."""

    def __init__(self, root, order):
        self.inner = LocalRawStore(root)
        self.order = order
        self.gate = threading.Event()
        self.gate.set()
        self.fail = False

    def save_raw(self, raw, meta):
        self.order.append("save:start")
        self.gate.wait(10)
        if self.fail:
            raise OSError("disk full")
        saved = self.inner.save_raw(raw, meta)
        self.order.append("save:done")
        return saved

    def load_raw(self, key):
        return self.inner.load_raw(key)


class GatedPublisher:
    def __init__(self, order):
        self.order = order
        self.events = []
        self.gate = threading.Event()
        self.gate.set()
        self.fail = False

    def publish(self, event):
        self.order.append("publish:start")
        self.gate.wait(10)
        if self.fail:
            raise ConnectionError("redis down")
        self.events.append(event)
        self.order.append("publish:done")
        return f"{len(self.events)}-0"


def frame(payload):
    return b"\x0b" + payload + b"\x1c\r"


def msa(ack: bytes) -> list[str]:
    """MSA fields of an ACK, e.g. ["MSA", "AA", "CTRL0001"]."""
    segment = next(s for s in ack.split(b"\r") if s.startswith(b"MSA"))
    return segment.decode("latin-1").split("|")


async def wait_until(condition, timeout=5.0):
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.01)


def run_with_listener(store, publisher, body):
    """Start the listener, connect a client, run `body(reader, writer)`, clean up."""

    async def main():
        listener = Listener(store, publisher)
        server = await start_hl7_server(listener.handle_connection, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            return await body(reader, writer)
        finally:
            store.gate.set()
            publisher.gate.set()
            writer.close()
            server.close()
            await listener.close_connections()

    return asyncio.run(main())


@pytest.fixture
def order():
    return []


@pytest.fixture
def store(tmp_path, order):
    return GatedStore(tmp_path, order)


@pytest.fixture
def publisher(order):
    return GatedPublisher(order)


def test_happy_path_order_is_save_publish_ack(store, publisher, order):
    async def body(reader, writer):
        writer.write(frame(MSG))
        ack = await asyncio.wait_for(read_frame(reader), 5)
        order.append("ack:received")
        return ack

    ack = run_with_listener(store, publisher, body)
    assert msa(ack)[:3] == ["MSA", "AA", "CTRL0001"]
    assert order == ["save:start", "save:done", "publish:start", "publish:done", "ack:received"]


def test_no_ack_while_save_is_in_progress(store, publisher, order):
    store.gate.clear()

    async def body(reader, writer):
        writer.write(frame(MSG))
        await wait_until(lambda: "save:start" in order)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(read_frame(reader), 0.3)
        assert "publish:start" not in order  # never publish before the save is done

        store.gate.set()
        return await asyncio.wait_for(read_frame(reader), 5)

    assert msa(run_with_listener(store, publisher, body))[1] == "AA"


def test_no_ack_while_publish_is_in_progress(store, publisher, order, tmp_path):
    publisher.gate.clear()

    async def body(reader, writer):
        writer.write(frame(MSG))
        await wait_until(lambda: "publish:start" in order)
        # The raw file is already on disk, but that alone is not enough to ACK.
        assert len(list(tmp_path.rglob("*.hl7"))) == 1
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(read_frame(reader), 0.3)

        publisher.gate.set()
        return await asyncio.wait_for(read_frame(reader), 5)

    assert msa(run_with_listener(store, publisher, body))[1] == "AA"


def test_save_failure_sends_ae_and_publishes_nothing(store, publisher, order):
    store.fail = True

    async def body(reader, writer):
        writer.write(frame(MSG))
        return await asyncio.wait_for(read_frame(reader), 5)

    ack = run_with_listener(store, publisher, body)
    assert msa(ack)[:3] == ["MSA", "AE", "CTRL0001"]
    assert publisher.events == []
    assert "publish:start" not in order


def test_publish_failure_sends_ae(store, publisher, tmp_path):
    publisher.fail = True

    async def body(reader, writer):
        writer.write(frame(MSG))
        return await asyncio.wait_for(read_frame(reader), 5)

    ack = run_with_listener(store, publisher, body)
    assert msa(ack)[1] == "AE"
    # The raw file stays: when the sender resends, we get a second copy. A duplicate
    # is fine; a message that is ACKed but never queued would not be.
    assert len(list(tmp_path.rglob("*.hl7"))) == 1


def test_connection_keeps_working_after_an_ae(store, publisher):
    publisher.fail = True

    async def body(reader, writer):
        writer.write(frame(MSG))
        first = await asyncio.wait_for(read_frame(reader), 5)
        publisher.fail = False
        writer.write(frame(MSG))  # the sender resends the same message
        second = await asyncio.wait_for(read_frame(reader), 5)
        return first, second

    first, second = run_with_listener(store, publisher, body)
    assert (msa(first)[1], msa(second)[1]) == ("AE", "AA")
    assert len(publisher.events) == 1


def test_unreadable_message_is_still_stored_queued_and_acked(store, publisher, tmp_path):
    junk = b"this is not HL7 \xff\xfe"

    async def body(reader, writer):
        writer.write(frame(junk))
        return await asyncio.wait_for(read_frame(reader), 5)

    ack = run_with_listener(store, publisher, body)
    assert msa(ack)[1] == "AA"
    [event] = publisher.events
    assert event["raw_key"].startswith("hospital=unknown/feed=unknown/")
    assert store.load_raw(event["raw_key"]) == junk


def test_several_messages_on_one_connection_each_get_their_own_ack(store, publisher):
    async def body(reader, writer):
        # Both frames in one write, with a stray newline between them.
        writer.write(frame(MSG) + b"\n" + frame(MSG2))
        return [await asyncio.wait_for(read_frame(reader), 5) for _ in range(2)]

    acks = run_with_listener(store, publisher, body)
    assert [msa(a)[2] for a in acks] == ["CTRL0001", "CTRL0002"]


def test_event_points_at_the_exact_raw_bytes(store, publisher):
    async def body(reader, writer):
        writer.write(frame(MSG))
        return await asyncio.wait_for(read_frame(reader), 5)

    run_with_listener(store, publisher, body)
    [event] = publisher.events
    assert store.load_raw(event["raw_key"]) == MSG
    assert event["raw_message_id"] == hashlib.sha256(MSG).hexdigest()
    assert event["raw_key"].endswith(f"_{event['raw_message_id'][:12]}.hl7")
    assert event["hospital"] == "synth_general"
    assert event["feed"] == "adt"
    assert event["control_id"] == "CTRL0001"
    assert event["source_system"] == "hl7v2"


def test_vt_byte_inside_a_message_is_stored_whole_and_acked_correctly(store, publisher):
    """Regression: a stray 0x0B in free text must not cut the message. Previously only
    the bytes after it were stored, and the AA had an empty control ID."""
    msg = MSG + b"OBX|1|TX|NOTE||line one\x0bline two\r"

    async def body(reader, writer):
        writer.write(frame(msg))
        return await asyncio.wait_for(read_frame(reader), 5)

    ack = run_with_listener(store, publisher, body)
    assert msa(ack)[:3] == ["MSA", "AA", "CTRL0001"]
    [event] = publisher.events
    assert store.load_raw(event["raw_key"]) == msg
