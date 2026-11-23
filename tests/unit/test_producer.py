import asyncio
from datetime import datetime, timezone
from unittest import mock

import pytest
from hl7.mllp import start_hl7_server

from producer.messages import MessageGenerator
from producer.sender import MllpSender, SendFailed
from services.listener.msh import read_msh

FIXED = datetime(2026, 11, 20, tzinfo=timezone.utc)
MSG = MessageGenerator(seed=1, run_id="T").next()


def test_generated_messages_are_synthetic_and_readable():
    messages = MessageGenerator(seed=1, run_id="T").take(200)

    assert len({m.control_id for m in messages}) == 200
    assert {m.message_type for m in messages} == {"ADT^A01", "ADT^A08", "ADT^A03", "ORU^R01"}
    for m in messages:
        assert m.raw.endswith(b"\r") and b"\n" not in m.raw
        m.raw.decode("ascii")  # plain ASCII
        header = read_msh(m.raw)
        assert header.error is None
        assert header.control_id == m.control_id
        assert header.facility_id == "SYNTH_GENERAL"
        assert b"|SYN" in m.raw  # MRNs are clearly synthetic


def test_same_seed_same_content():
    # Freeze the clock, so only the seed decides the content.
    with mock.patch("producer.messages.datetime") as frozen:
        frozen.now.return_value = FIXED
        a = MessageGenerator(seed=7, run_id="X").take(20)
        b = MessageGenerator(seed=7, run_id="X").take(20)
    assert b"|20261120000000+0000|" in a[0].raw  # the frozen time was used
    assert [m.raw for m in a] == [m.raw for m in b]


def ack(code: str, control_id: str) -> bytes:
    return f"MSH|^~\\&|CDP|CDP|P|F|20261120||ACK|A1|P|2.5.1\rMSA|{code}|{control_id}\r".encode()


def send_against(script):
    """Send MSG to a fake receiver that answers each message it receives with the next
    action in `script` (an ACK code, or "drop" to close without ACKing). Returns the
    sender's attempt count and the bytes the receiver got."""
    received = []

    async def handle(reader, writer):
        try:
            while script:
                raw = await reader.readblock()
                received.append(raw)
                action = script.pop(0)
                if action == "drop":
                    writer.close()  # crash without ACKing
                    return
                writer.writeblock(ack(action, raw.split(b"|")[9].decode()))
                await writer.drain()
        except asyncio.IncompleteReadError:
            pass

    async def main():
        server = await start_hl7_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        sender = MllpSender("127.0.0.1", port)
        try:
            # The sender resends forever, so stop the test if it never accepts an AA.
            return await asyncio.wait_for(sender.send(MSG.raw, MSG.control_id), timeout=10)
        finally:
            await sender.close()
            server.close()

    with mock.patch("producer.sender.RETRY_DELAY", 0.01):  # no real waiting between resends
        return asyncio.run(main()), received


def test_sender_returns_after_aa():
    attempts, received = send_against(["AA"])
    assert attempts == 1
    assert received == [MSG.raw]


def test_sender_resends_after_ae():
    attempts, received = send_against(["AE", "AA"])
    assert attempts == 2
    assert received == [MSG.raw, MSG.raw]  # the exact same bytes again


def test_sender_resends_when_connection_drops_before_ack():
    attempts, received = send_against(["drop", "AA"])
    assert attempts == 2
    assert received == [MSG.raw, MSG.raw]


def test_sender_gives_up_on_ar():
    with pytest.raises(SendFailed, match="rejected"):
        send_against(["AR"])
