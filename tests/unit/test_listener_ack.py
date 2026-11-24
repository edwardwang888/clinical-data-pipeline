"""The listener's core promise: AA is sent only after the save AND the publish succeed.

These tests run the real listener on a local TCP port, with a raw store and publisher
that can be paused or made to fail, and watch what the sender receives. Messages the
listener can't identify get AR, after their bytes are kept as evidence.
"""

import asyncio
import base64
import hashlib
import logging
import tempfile
import threading

import pytest
from hl7.mllp import open_hl7_connection, start_hl7_server

from pipeline.raw_store import LocalRawStore
from services.listener.server import Listener

MSG = b"MSH|^~\\&|SIMHOSP|SYNTH_GENERAL|CDP|CDP|20261120225047||ADT^A01|CTRL0001|P|2.5.1\rPID|||SYN000001\r"
MSG2 = MSG.replace(b"CTRL0001", b"CTRL0002")


class GatedStore(LocalRawStore):
    """Real LocalRawStore, but each save can be paused (gate) or made to fail."""

    def __init__(self, root, order):
        super().__init__(root)
        self.order = order
        self.gate = threading.Event()
        self.gate.set()
        self.fail = False

    def save_raw(self, raw, meta):
        self.order.append("save:start")
        self.gate.wait(10)
        if self.fail:
            raise OSError("disk full")
        saved = super().save_raw(raw, meta)
        self.order.append("save:done")
        return saved


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


class EvidenceStore(LocalRawStore):
    """Real LocalRawStore for rejected messages, recording what was saved."""

    def __init__(self, root):
        super().__init__(root)
        self.saved = []
        self.fail = False

    def save_raw(self, raw, meta):
        if self.fail:
            raise OSError("disk full")
        saved = super().save_raw(raw, meta)
        self.saved.append(saved)
        return saved


class DeadLetters:
    """Records what the listener publishes to the dead-letter queue (hl7.dlq)."""

    def __init__(self):
        self.entries = []
        self.fail = False

    def publish(self, event):
        if self.fail:
            raise ConnectionError("redis down")
        self.entries.append(event)
        return f"{len(self.entries)}-0"


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


async def read_reply(reader, timeout=5.0):
    """The listener's next reply (an ACK), without its MLLP framing."""
    return await asyncio.wait_for(reader.readblock(), timeout)


async def assert_no_reply(reader):
    with pytest.raises(TimeoutError):
        await read_reply(reader, timeout=0.3)


def run_with_listener(store, publisher, body, rejected=None, dlq=None):
    """Start the listener, connect a client, run `body(reader, writer)`, clean up.

    Tests that don't pass `rejected` and `dlq` are about valid input: for those we check
    afterwards that nothing reached the evidence area or the dead-letter queue.
    """
    expect_no_dead_letters = rejected is None and dlq is None
    rejected = rejected or EvidenceStore(tempfile.mkdtemp())
    dlq = dlq or DeadLetters()

    async def main():
        listener = Listener(store, publisher, rejected, dlq)
        server = await start_hl7_server(listener.handle_connection, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        reader, writer = await open_hl7_connection("127.0.0.1", port)
        try:
            return await body(reader, writer)
        finally:
            store.gate.set()
            publisher.gate.set()
            writer.close()
            server.close()
            await listener.close_connections()

    result = asyncio.run(main())
    if expect_no_dead_letters:
        assert rejected.saved == [], "a valid message was rejected or treated as bad input"
        assert dlq.entries == [], "a valid message reached the dead-letter queue"
    return result


def send(store, publisher, data, rejected=None, dlq=None):
    """Send `data` on one connection and return the listener's reply."""

    async def body(reader, writer):
        writer.write(data)
        return await read_reply(reader)

    return run_with_listener(store, publisher, body, rejected, dlq)


@pytest.fixture
def order():
    return []


@pytest.fixture
def store(tmp_path, order):
    return GatedStore(tmp_path, order)


@pytest.fixture
def publisher(order):
    return GatedPublisher(order)


@pytest.fixture
def rejected(tmp_path_factory):
    # A separate folder from the raw archive (tmp_path), like data/rejected vs data/raw.
    return EvidenceStore(tmp_path_factory.mktemp("rejected"))


@pytest.fixture
def dlq():
    return DeadLetters()


def assert_dead_letter(entry, saved, data, kind, reply):
    """A DLQ entry points at the evidence file and identifies the exact bytes."""
    assert entry["stage"] == "listener"
    assert entry["kind"] == kind
    assert entry["reply"] == reply
    assert entry["reason"]
    assert entry["evidence_key"] == saved.key
    assert entry["raw_message_id"] == hashlib.sha256(data).hexdigest()
    assert entry["size_bytes"] == len(data)
    assert "payload_b64" not in entry  # the bytes are in the evidence file


def test_happy_path_order_is_save_publish_ack(store, publisher, order):
    async def body(reader, writer):
        writer.write(frame(MSG))
        ack = await read_reply(reader)
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
        await assert_no_reply(reader)
        assert "publish:start" not in order  # never publish before the save is done

        store.gate.set()
        return await read_reply(reader)

    assert msa(run_with_listener(store, publisher, body))[1] == "AA"


def test_no_ack_while_publish_is_in_progress(store, publisher, order, tmp_path):
    publisher.gate.clear()

    async def body(reader, writer):
        writer.write(frame(MSG))
        await wait_until(lambda: "publish:start" in order)
        # The raw file is already on disk, but that alone is not enough to ACK.
        assert len(list(tmp_path.rglob("*.hl7"))) == 1
        await assert_no_reply(reader)

        publisher.gate.set()
        return await read_reply(reader)

    assert msa(run_with_listener(store, publisher, body))[1] == "AA"


def test_save_failure_sends_ae_and_publishes_nothing(store, publisher, order):
    store.fail = True

    ack = send(store, publisher, frame(MSG))
    assert msa(ack)[:3] == ["MSA", "AE", "CTRL0001"]
    assert publisher.events == []
    assert "publish:start" not in order


def test_publish_failure_sends_ae(store, publisher, tmp_path):
    publisher.fail = True

    ack = send(store, publisher, frame(MSG))
    assert msa(ack)[1] == "AE"
    # The raw file was saved; it's the queueing that failed, and the text says so.
    assert msa(ack)[3] == "message not queued; please resend"
    # The raw file stays: when the sender resends, we get a second copy. A duplicate
    # is fine; a message that is ACKed but never queued would not be.
    assert len(list(tmp_path.rglob("*.hl7"))) == 1


def test_connection_keeps_working_after_an_ae(store, publisher):
    publisher.fail = True

    async def body(reader, writer):
        writer.write(frame(MSG))
        first = await read_reply(reader)
        publisher.fail = False
        writer.write(frame(MSG))  # the sender resends the same message
        second = await read_reply(reader)
        return first, second

    first, second = run_with_listener(store, publisher, body)
    assert (msa(first)[1], msa(second)[1]) == ("AE", "AA")
    assert len(publisher.events) == 1


@pytest.mark.parametrize(
    "junk",
    [
        b"this is not HL7 \xff\xfe",
        b"",
        b"MSH|^~\\&|SIMHOSP|SYNTH_GENERAL|CDP|CDP|20261120225047||ADT^A01||P|2.5.1\r",
    ],
    ids=["not-hl7", "empty-frame", "no-control-id"],
)
def test_unidentifiable_message_is_kept_as_evidence_and_rejected(store, publisher, rejected, dlq, tmp_path, junk):
    """No readable MSH, control ID or message type: keep the exact bytes as evidence,
    record it in the dead-letter queue, never queue it for processing, and reply AR so
    the sender stops resending."""
    ack = send(store, publisher, frame(junk), rejected, dlq)
    assert msa(ack)[1] == "AR"
    assert msa(ack)[3].startswith("rejected")
    assert publisher.events == []
    assert list(tmp_path.rglob("*.hl7")) == []  # nothing in the raw archive
    [saved] = rejected.saved
    assert rejected.load_raw(saved.key) == junk
    [entry] = dlq.entries
    assert_dead_letter(entry, saved, junk, kind="unidentifiable", reply="AR")


def test_reject_waits_for_the_evidence_to_be_saved(store, publisher, rejected, dlq):
    # We never answer before the bytes are stored, rejections included: if the
    # evidence can't be saved, reply AE so the sender tries again.
    rejected.fail = True

    ack = send(store, publisher, frame(b"not hl7"), rejected, dlq)
    assert msa(ack)[1] == "AE"
    assert publisher.events == []


def test_reject_waits_for_the_dead_letter_entry(store, publisher, rejected, dlq):
    # AR is sent only once the evidence is saved AND the DLQ entry is published; if the
    # DLQ is down, reply AE so the sender tries again.
    dlq.fail = True

    ack = send(store, publisher, frame(b"not hl7"), rejected, dlq)
    assert msa(ack)[1] == "AE"


def test_byte_order_mark_before_msh_is_accepted_and_stored_exactly(store, publisher):
    # Some senders prefix a UTF-8 BOM. The header is still readable, so this is a
    # valid message: AA, queued, and stored with the BOM untouched.
    msg = b"\xef\xbb\xbf" + MSG

    ack = send(store, publisher, frame(msg))
    assert msa(ack)[:3] == ["MSA", "AA", "CTRL0001"]
    [event] = publisher.events
    assert store.load_raw(event["raw_key"]) == msg


def test_several_messages_on_one_connection_each_get_their_own_ack(store, publisher):
    async def body(reader, writer):
        # Both frames in one write, with a stray newline between them.
        writer.write(frame(MSG) + b"\n" + frame(MSG2))
        return [await read_reply(reader) for _ in range(2)]

    acks = run_with_listener(store, publisher, body)
    assert [msa(a)[2] for a in acks] == ["CTRL0001", "CTRL0002"]


def test_event_points_at_the_exact_raw_bytes(store, publisher):
    send(store, publisher, frame(MSG))
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

    ack = send(store, publisher, frame(msg))
    assert msa(ack)[:3] == ["MSA", "AA", "CTRL0001"]
    [event] = publisher.events
    assert store.load_raw(event["raw_key"]) == msg


@pytest.mark.parametrize(
    "before",
    [
        b"\x0bMSH|partial",
        b"\x0bPING\r\n",
        b"\x0bMSH|^~\\&|SIMHOSP|SYNTH_GEN",
        b"\x0bMSH|^~\\&|SIMHOSP|SYNTH_GENERAL|CDP|CDP|20261120225047||ADT^A01|OLD0001|P|2.5.1\rPID|||SY",
    ],
    ids=["partial-msh", "noise-with-vt", "cut-inside-msh", "cut-after-msh"],
)
def test_message_after_an_abandoned_frame_gets_its_own_aa(store, publisher, rejected, dlq, before):
    """Regression: the sender started a frame, gave up, then sent a valid message on the
    same connection. The valid message must be stored whole and get its own AA. It
    previously got AR (or an ACK for the fragment's control ID) and never got through."""

    async def body(reader, writer):
        writer.write(before + frame(MSG))
        ack = await read_reply(reader)
        # Only one reply: the abandoned fragment was never completed, so it gets none.
        await assert_no_reply(reader)
        return ack

    ack = run_with_listener(store, publisher, body, rejected, dlq)
    assert msa(ack)[:3] == ["MSA", "AA", "CTRL0001"]
    [event] = publisher.events
    assert store.load_raw(event["raw_key"]) == MSG
    # The fragment is kept as evidence (minus its <VT> framing byte) and dead-lettered.
    [saved] = rejected.saved
    assert rejected.load_raw(saved.key) == before[1:]
    [entry] = dlq.entries
    assert_dead_letter(entry, saved, before[1:], kind="abandoned_frame", reply="none")


# --- nothing is dropped silently ------------------------------------------------------


@pytest.mark.parametrize(
    "before, kind",
    [(b"garbage", "noise_before_frame"), (b"MSH|no start byte\x1c\r", "no_start_byte")],
    ids=["noise-before-frame", "block-without-start-byte"],
)
def test_unusable_bytes_are_kept_as_evidence(store, publisher, rejected, dlq, before, kind):
    async def body(reader, writer):
        writer.write(before + frame(MSG))
        ack = await read_reply(reader)
        await assert_no_reply(reader)  # one reply only: the valid message's
        return ack

    ack = run_with_listener(store, publisher, body, rejected, dlq)
    assert msa(ack)[:3] == ["MSA", "AA", "CTRL0001"]
    [event] = publisher.events
    assert store.load_raw(event["raw_key"]) == MSG
    [saved] = rejected.saved
    assert rejected.load_raw(saved.key) == before  # exact bytes as received
    [entry] = dlq.entries
    assert_dead_letter(entry, saved, before, kind=kind, reply="none")


def test_whitespace_between_frames_is_not_kept(store, publisher, rejected, dlq):
    async def body(reader, writer):
        writer.write(b"\r\n" + frame(MSG) + b"\r\n" + frame(MSG2))
        return [await read_reply(reader) for _ in range(2)]

    acks = run_with_listener(store, publisher, body, rejected, dlq)
    assert [msa(a)[1] for a in acks] == ["AA", "AA"]
    assert rejected.saved == []
    assert dlq.entries == []


def test_frame_cut_off_by_a_disconnect_is_kept_as_evidence(store, publisher, rejected, dlq):
    cut = b"\x0bMSH|^~\\&|SIMHOSP|SYNTH_GENERAL|CDP|CDP|2026"

    async def body(reader, writer):
        writer.write(frame(MSG) + cut)
        ack = await read_reply(reader)
        writer.close()  # the sender disconnects before finishing the second frame
        await wait_until(lambda: dlq.entries)
        return ack

    ack = run_with_listener(store, publisher, body, rejected, dlq)
    assert msa(ack)[1] == "AA"
    [saved] = rejected.saved
    assert rejected.load_raw(saved.key) == cut
    [entry] = dlq.entries
    assert_dead_letter(entry, saved, cut, kind="cut_off_by_disconnect", reply="none")
    assert len(publisher.events) == 1  # only the complete message was queued


def test_oversized_frame_is_kept_as_evidence_and_the_connection_closed(store, publisher, rejected, dlq):
    # The test server uses python-hl7's default 64 KiB limit; send more than that.
    big = MSG + b"NTE|1||" + b"x" * 100_000 + b"\r"

    async def body(reader, writer):
        writer.write(frame(big))
        try:
            reply = await read_reply(reader)
        except (ConnectionError, asyncio.IncompleteReadError):
            reply = None  # the listener closed the connection without replying
        await wait_until(lambda: dlq.entries)
        return reply

    assert run_with_listener(store, publisher, body, rejected, dlq) is None  # no ACK
    [saved] = rejected.saved
    kept = rejected.load_raw(saved.key)
    assert kept.startswith(b"\x0b" + MSG)
    [entry] = dlq.entries
    assert_dead_letter(entry, saved, kept, kind="too_large", reply="none")
    assert publisher.events == []


def test_failed_evidence_save_puts_the_bytes_in_the_dead_letter_entry(store, publisher, rejected, dlq):
    # If the evidence file can't be saved, the DLQ entry carries the bytes themselves,
    # and the valid message after them is unaffected.
    rejected.fail = True

    ack = send(store, publisher, b"garbage" + frame(MSG), rejected, dlq)
    assert msa(ack)[:3] == ["MSA", "AA", "CTRL0001"]
    [entry] = dlq.entries
    assert entry["evidence_key"] == ""
    assert base64.b64decode(entry["payload_b64"]) == b"garbage"
    assert entry["raw_message_id"] == hashlib.sha256(b"garbage").hexdigest()


def test_dead_letter_queue_down_falls_back_to_the_log(store, publisher, rejected, dlq, caplog):
    # Last resort, only when the DLQ itself is unreachable: the log records the bytes'
    # size, hash and where the evidence file is, so the input is still traceable.
    dlq.fail = True
    caplog.set_level(logging.ERROR)

    ack = send(store, publisher, b"garbage" + frame(MSG), rejected, dlq)
    assert msa(ack)[:3] == ["MSA", "AA", "CTRL0001"]
    [saved] = rejected.saved
    assert hashlib.sha256(b"garbage").hexdigest() in caplog.text
    assert saved.key in caplog.text
