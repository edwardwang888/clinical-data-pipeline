import asyncio
from datetime import datetime, timezone

import pytest
from hl7.mllp import start_hl7_server

from producer.messages import MessageGenerator
from producer.sender import MllpSender, SendFailed
from services.listener.msh import read_msh

FIXED = datetime(2026, 11, 20, tzinfo=timezone.utc)


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
    a = MessageGenerator(seed=7, run_id="X", clock=lambda: FIXED).take(20)
    b = MessageGenerator(seed=7, run_id="X", clock=lambda: FIXED).take(20)
    assert [m.raw for m in a] == [m.raw for m in b]


def ack(code: str, control_id: str) -> bytes:
    return f"MSH|^~\\&|CDP|CDP|P|F|20261120||ACK|A1|P|2.5.1\rMSA|{code}|{control_id}\r".encode()


def run_against(script, body):
    """Run a fake receiver that answers connection by connection, message by message,
    following `script` (a list of actions per received message)."""
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
        sender = MllpSender("127.0.0.1", port, retry_delay=0.01, ack_timeout=2, max_attempts=5)
        try:
            return await body(sender)
        finally:
            await sender.close()
            server.close()

    return asyncio.run(main()), received


MSG = MessageGenerator(seed=1, run_id="T").next()


def test_sender_returns_after_aa():
    attempts, received = run_against(["AA"], lambda s: s.send(MSG.raw, MSG.control_id))
    assert attempts == 1
    assert received == [MSG.raw]


def test_sender_resends_after_ae():
    attempts, received = run_against(["AE", "AA"], lambda s: s.send(MSG.raw, MSG.control_id))
    assert attempts == 2
    assert received == [MSG.raw, MSG.raw]  # the exact same bytes again


def test_sender_resends_when_connection_drops_before_ack():
    attempts, received = run_against(["drop", "AA"], lambda s: s.send(MSG.raw, MSG.control_id))
    assert attempts == 2
    assert received == [MSG.raw, MSG.raw]


def test_sender_gives_up_on_ar():
    with pytest.raises(SendFailed, match="rejected"):
        run_against(["AR"], lambda s: s.send(MSG.raw, MSG.control_id))


def test_sender_gives_up_after_max_attempts():
    with pytest.raises(SendFailed, match="5 attempts"):
        run_against(["AE"] * 5, lambda s: s.send(MSG.raw, MSG.control_id))
