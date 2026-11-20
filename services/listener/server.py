"""The MLLP listener: receive -> store raw bytes -> publish event -> ACK.

The one rule this module exists to enforce: **never ACK a message we could still
lose.** An AA goes back to the sender only after the raw bytes are fsynced to the
archive AND the queue event is published. If either step fails we send AE (and the
sender resends); if we crash before ACKing, the sender times out and resends. Either
way the worst case is a duplicate, which downstream consumers handle. A loss is not
possible.

The listener does not parse. It reads only the few MSH fields it needs (see msh.py),
and stores the message even when it can't read those.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from pipeline.publisher import Publisher
from pipeline.raw_store import RawStore, safe_segment
from services.listener.ack import build_ack
from services.listener.framing import FramingError, FrameTooLarge, read_frame
from services.listener.msh import read_msh

log = logging.getLogger(__name__)

LISTENER_VERSION = "0.1.0"
SOURCE_SYSTEM = "hl7v2"


class Listener:
    def __init__(self, store: RawStore, publisher: Publisher) -> None:
        self.store = store
        self.publisher = publisher
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
        """
        received_at = datetime.now(timezone.utc)
        header = read_msh(raw)
        if header.error:
            log.warning("unreadable header peer=%s: %s (storing anyway)", peer, header.error)

        meta = {
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

    async def close_connections(self) -> None:
        """Stop serving open connections (used on shutdown). In-flight messages that
        were not ACKed yet will be resent by their senders."""
        for task in list(self._connections):
            task.cancel()
        await asyncio.gather(*self._connections, return_exceptions=True)


def _format_peer(peername) -> str:
    if isinstance(peername, tuple) and len(peername) >= 2:
        return f"{peername[0]}:{peername[1]}"
    return str(peername)
