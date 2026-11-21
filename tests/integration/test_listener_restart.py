"""Milestone 1 "done" test: restart the listener mid-stream and lose nothing.

Sends 1,000 messages over 4 concurrent connections to a real listener process, kills it
partway through (a crash with SIGKILL, and separately a clean stop with SIGTERM),
starts it again, and lets the senders resend whatever wasn't ACKed.

Then checks the promise: every ACKed message has a raw file with its exact bytes and a
queue event pointing at such a file. Duplicates are fine; losses are not.
"""

import asyncio
import hashlib
import json
import signal
from collections import defaultdict

import pytest

from pipeline.raw_store import LocalRawStore
from producer.messages import MessageGenerator
from producer.sender import MllpSender
from tests.conftest import ListenerProcess, free_port

pytestmark = pytest.mark.integration

N_MESSAGES = 1000
N_CONNECTIONS = 4
RESTART_AFTER_ACKS = 400


@pytest.mark.parametrize("sig", [signal.SIGKILL, signal.SIGTERM], ids=["crash", "clean-stop"])
def test_no_acked_message_is_lost_when_listener_restarts(
    tmp_path, redis_url, redis_client, stream_name, sig
):
    port = free_port()
    listener = ListenerProcess(port, tmp_path, redis_url, stream_name)
    raw_dir = listener.raw_dir
    messages = MessageGenerator(seed=2026, run_id="IT").take(N_MESSAGES)
    sent_bytes = {m.raw for m in messages}
    acked: dict[str, bytes] = {}  # control_id -> exact bytes, recorded only on AA

    async def send_batch(batch):
        sender = MllpSender("127.0.0.1", port, retry_delay=0.05, max_retry_delay=0.5, ack_timeout=5)
        try:
            for m in batch:
                await sender.send(m.raw, m.control_id)
                acked[m.control_id] = m.raw
        finally:
            await sender.close()
        return sender.resends

    async def restart_midway():
        while len(acked) < RESTART_AFTER_ACKS:
            await asyncio.sleep(0.002)
        await asyncio.to_thread(listener.stop, sig)
        await asyncio.sleep(0.3)  # downtime: senders get "connection refused" and back off
        await asyncio.to_thread(listener.start)

    async def main():
        batches = [messages[i::N_CONNECTIONS] for i in range(N_CONNECTIONS)]
        _, *resends = await asyncio.wait_for(
            asyncio.gather(restart_midway(), *(send_batch(b) for b in batches)), timeout=300
        )
        return sum(resends)

    listener.start()
    try:
        resends = asyncio.run(main())
    finally:
        listener.stop()

    # The restart really happened mid-stream, and senders had to resend.
    assert listener.starts == 2
    assert resends > 0
    # Senders keep resending until AA, so in the end everything was acknowledged.
    assert len(acked) == N_MESSAGES

    store = LocalRawStore(raw_dir)

    # No truncated or foreign file ever appears under a final name: every .hl7 file
    # holds exactly the bytes of a message we sent. (A crash can leave .tmp-* files.)
    hl7_files = list(raw_dir.rglob("*.hl7"))
    assert not listener.rejected_dir.exists()  # every message was valid: none rejected
    assert redis_client.xlen(f"{stream_name}.dlq") == 0  # and nothing was dead-lettered
    assert all(p.read_bytes() in sent_bytes for p in hl7_files)

    # Raw archive: every ACKed message has a file (with metadata) holding its exact bytes.
    stored = defaultdict(list)
    for meta_path in raw_dir.rglob("*.json"):
        meta = json.loads(meta_path.read_text())
        raw = store.load_raw(meta["raw_key"])
        assert hashlib.sha256(raw).hexdigest() == meta["sha256"]
        stored[meta["control_id"]].append(raw)
    missing_raw = [cid for cid, raw in acked.items() if raw not in stored[cid]]
    assert missing_raw == [], f"{len(missing_raw)} ACKed messages have no raw file"

    # Queue: every ACKed message has an event pointing at a file with its exact bytes.
    events = redis_client.xrange(stream_name)
    queued = defaultdict(list)
    for _event_id, fields in events:
        raw = store.load_raw(fields["raw_key"])
        assert hashlib.sha256(raw).hexdigest() == fields["raw_message_id"]
        queued[fields["control_id"]].append(raw)
    missing_events = [cid for cid, raw in acked.items() if raw not in queued[cid]]
    assert missing_events == [], f"{len(missing_events)} ACKed messages have no queue event"

    duplicates = len(events) - N_MESSAGES
    print(
        f"\n[{signal.Signals(sig).name}] {N_MESSAGES} ACKed, {resends} resends, "
        f"{len(hl7_files)} raw files, {len(events)} events ({duplicates} duplicates), "
        f"{len(list(raw_dir.rglob('.tmp-*')))} leftover temp files"
    )
