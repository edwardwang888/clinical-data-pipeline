"""The MLLP listener: receive -> store raw bytes -> publish event -> ACK.

The one rule this module exists to enforce: **never ACK a message we could still
lose.** An AA goes back to the sender only after the raw bytes are fsynced to the
archive AND the queue event is published. If either step fails we send AE (and the
sender resends); if we crash before ACKing, the sender times out and resends. Either
way the worst case is a duplicate, which downstream consumers handle. A loss is not
possible.

The listener does not parse. It reads only the few MSH fields it needs (see msh.py).
If it can't read them, it can't identify the message: there is no control ID the
sender could match an ACK to, and nothing to queue it under. It then keeps the exact
bytes as evidence in a separate "rejected" store, never queues them, and replies AR
("rejected, don't resend") so the sender's team can investigate. Messages with a
readable header are always stored and ACKed, however wrong their contents; the parser
quarantines those later, so problems inside a message never stall the feed.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from pipeline.publisher import Publisher
from pipeline.raw_store import RawStore, safe_segment
from services.listener.ack import build_ack
from services.listener.framing import FramingError, FrameTooLarge, read_frame, split_abandoned
from services.listener.msh import MshHeader, read_msh

log = logging.getLogger(__name__)

LISTENER_VERSION = "0.1.0"
SOURCE_SYSTEM = "hl7v2"


class Listener:
    def __init__(self, store: RawStore, publisher: Publisher, rejected_store: RawStore) -> None:
        self.store = store  # the raw archive: every message we can identify
        self.publisher = publisher
        self.rejected_store = rejected_store  # evidence for messages we reject (AR)
        self._connections: set[asyncio.Task] = set()

    async def handle_connection(self, reader: asyncio.StreamReader, writer) -> None:
        """Serve one sender connection: one message at a time, each answered by an ACK.

        `writer` is python-hl7's HL7StreamWriter; `writeblock` adds the MLLP framing.
        """
        task = asyncio.current_task()
        self._connections.add(task)
        peer = _format_peer(writer.get_extra_info("peername"))
        log.info("connection opened peer=%s", peer)
        try:
            while True:
                try:
                    raw = await read_frame(reader)
                except FrameTooLarge as e:
                    log.error("closing connection peer=%s: %s", peer, e)
                    break
                except FramingError as e:
                    log.warning("skipping invalid frame peer=%s: %s", peer, e)
                    continue
                if raw is None:
                    break  # sender closed the connection

                # If the sender abandoned a frame and started a new one, handle the new
                # message on its own; the abandoned part is kept as evidence, no reply.
                abandoned, raw = split_abandoned(raw)
                if abandoned:
                    await self._keep_abandoned(abandoned, peer)

                ack = await self.handle_message(raw, peer)
                writer.writeblock(ack)
                await writer.drain()
        except ConnectionError as e:
            # Sender went away. Anything not yet ACKed will be resent by it.
            log.info("connection lost peer=%s: %s", peer, e)
        except Exception:
            log.exception("unexpected error, closing connection peer=%s", peer)
        finally:
            self._connections.discard(task)
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, asyncio.CancelledError):
                pass
            log.info("connection closed peer=%s", peer)

    async def handle_message(self, raw: bytes, peer: str) -> bytes:
        """Store `raw`, publish its event, and return the ACK bytes to send back.

        Returns AA only if both the save and the publish succeeded; otherwise AE.
        A message whose header can't be read is kept as evidence and gets AR.
        """
        received_at = datetime.now(timezone.utc)
        header = read_msh(raw)
        meta = _metadata(header, received_at, peer)
        if header.error:
            return await self._reject(raw, header, meta, peer)

        try:
            # File I/O and the Redis call block, so run them in a worker thread to keep
            # other connections flowing meanwhile.
            saved = await asyncio.to_thread(self.store.save_raw, raw, meta)
            event = {
                "raw_key": saved.key,
                "raw_message_id": saved.sha256,
                "source_system": SOURCE_SYSTEM,
                "hospital": meta["hospital"],
                "feed": meta["feed"],
                "message_type": header.message_type,
                "control_id": header.control_id,
                "received_at": received_at.isoformat(),
            }
            event_id = await asyncio.to_thread(self.publisher.publish, event)
        except Exception:
            log.exception("NOT stored, sending AE peer=%s control_id=%s", peer, header.control_id)
            return build_ack(header, "AE", text="message not stored; please resend")

        log.info(
            "stored and queued control_id=%s type=%s key=%s event=%s",
            header.control_id,
            header.message_type,
            saved.key,
            event_id,
        )
        return build_ack(header, "AA")

    async def _reject(self, raw: bytes, header: MshHeader, meta: dict, peer: str) -> bytes:
        """Keep an unidentifiable message's bytes as evidence, then reply AR.

        Even a rejection is only sent once the bytes are stored: if saving the evidence
        fails we reply AE instead, and the sender tries again.
        """
        try:
            saved = await asyncio.to_thread(self.rejected_store.save_raw, raw, meta)
        except Exception:
            log.exception("could not keep rejected message, sending AE peer=%s", peer)
            return build_ack(header, "AE", text="message not stored; please resend")
        log.warning("rejected (AR) peer=%s reason=%s evidence=%s", peer, header.error, saved.key)
        return build_ack(header, "AR", text=f"rejected: {header.error}")

    async def _keep_abandoned(self, fragment: bytes, peer: str) -> None:
        """Keep the bytes of a frame the sender abandoned, as evidence. No reply.

        The sender never finished this frame, so it isn't waiting for an answer to it,
        and it will resend the message if it was real. A failure to save the evidence is
        only logged: it must never hold up the complete message that follows.
        """
        meta = _metadata(read_msh(fragment), datetime.now(timezone.utc), peer)
        meta["header_error"] = "abandoned partial frame: the sender started a new frame before finishing this one"
        try:
            saved = await asyncio.to_thread(self.rejected_store.save_raw, fragment, meta)
        except Exception:
            log.exception("could not keep abandoned frame peer=%s (%d bytes)", peer, len(fragment))
            return
        log.warning("abandoned partial frame peer=%s bytes=%d evidence=%s", peer, len(fragment), saved.key)

    async def close_connections(self) -> None:
        """Stop serving open connections (used on shutdown). In-flight messages that
        were not ACKed yet will be resent by their senders."""
        for task in list(self._connections):
            task.cancel()
        await asyncio.gather(*self._connections, return_exceptions=True)


def _metadata(header: MshHeader, received_at: datetime, peer: str) -> dict:
    """What we record next to every stored message (raw or rejected)."""
    return {
        "received_at": received_at,
        "hospital": safe_segment(header.facility_id),
        "feed": safe_segment(header.message_code),
        "peer": peer,
        "sending_facility": header.sending_facility,
        "message_type": header.message_type,
        "control_id": header.control_id,
        "header_error": header.error,
        "source_system": SOURCE_SYSTEM,
        "listener_version": LISTENER_VERSION,
    }


def _format_peer(peername) -> str:
    if isinstance(peername, tuple) and len(peername) >= 2:
        return f"{peername[0]}:{peername[1]}"
    return str(peername)
