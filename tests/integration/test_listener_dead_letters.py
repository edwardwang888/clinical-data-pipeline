"""Bad input goes to the dead-letter queue, end to end: the real listener process,
real Redis, real files. Unit tests cover each case with fakes; this checks the wiring
(the DLQ stream name, the evidence folder, the entry fields as stored in Redis)."""

import asyncio
import hashlib

import pytest
from hl7.mllp import open_hl7_connection

from pipeline.raw_store import LocalRawStore
from tests.conftest import ListenerProcess, free_port

pytestmark = pytest.mark.integration

VALID = b"MSH|^~\\&|SIM|SYNTH_GENERAL|CDP|CDP|20261121||ADT^A01|DLQ0001|P|2.5.1\rPID|||SYN000001\r"


def send(port, data):
    """Send raw bytes on one connection and return the replies until it goes quiet."""

    async def run():
        reader, writer = await open_hl7_connection("127.0.0.1", port)
        writer.write(data)
        replies = []
        try:
            while True:
                replies.append(await asyncio.wait_for(reader.readblock(), 1.0))
        except TimeoutError:
            pass
        writer.close()
        return replies

    return asyncio.run(run())


def test_bad_input_is_dead_lettered_with_its_evidence(tmp_path, redis_url, redis_client, stream_name):
    dlq_stream = f"{stream_name}.dlq"
    port = free_port()
    listener = ListenerProcess(port, tmp_path, redis_url, stream_name)
    listener.start()
    try:
        # Noise, then an unidentifiable frame, then a valid message, on one connection.
        replies = send(port, b"noise" + b"\x0bnot hl7 at all\x1c\r" + b"\x0b" + VALID + b"\x1c\r")
        entries = [fields for _id, fields in redis_client.xrange(dlq_stream)]
    finally:
        listener.stop()
        redis_client.delete(dlq_stream)

    # Replies: AR for the unidentifiable frame, AA for the valid one; the noise gets none.
    codes = [next(s for s in r.split(b"\r") if s.startswith(b"MSA")).split(b"|")[1] for r in replies]
    assert codes == [b"AR", b"AA"]

    # One DLQ entry per bad input, each pointing at its exact bytes in data/rejected.
    evidence = LocalRawStore(listener.rejected_dir)
    by_kind = {e["kind"]: e for e in entries}
    assert set(by_kind) == {"noise_before_frame", "unidentifiable"}
    assert by_kind["noise_before_frame"]["reply"] == "none"
    assert by_kind["unidentifiable"]["reply"] == "AR"
    for kind, data in [("noise_before_frame", b"noise"), ("unidentifiable", b"not hl7 at all")]:
        entry = by_kind[kind]
        assert entry["stage"] == "listener"
        assert evidence.load_raw(entry["evidence_key"]) == data
        assert entry["raw_message_id"] == hashlib.sha256(data).hexdigest()
        assert "payload_b64" not in entry

    # The valid message went to the raw archive and the normal stream, not the DLQ.
    assert redis_client.xlen(stream_name) == 1
    assert len(list(listener.raw_dir.rglob("*.hl7"))) == 1
