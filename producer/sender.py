"""Send messages over MLLP the way a hospital interface engine does.

One message at a time per connection: send, wait for the ACK, then the next. If the
ACK is AE, doesn't come within ACK_TIMEOUT, or the connection drops, close the
connection, wait a bit, reconnect and send *the same message again*, for as long as it
takes, like a real interface engine. The receiver may
already have stored it, so this can create duplicates. That's expected: "resend until
acknowledged" is what makes the feed lossless, and duplicates are handled downstream.
"""

from __future__ import annotations

import asyncio
import logging

import hl7
from hl7.mllp import open_hl7_connection

log = logging.getLogger(__name__)

ACK_TIMEOUT = 10.0  # seconds to wait for an ACK before resending
RETRY_DELAY = 0.5  # first wait before a resend; doubles after each failed attempt
MAX_RETRY_DELAY = 10.0


class SendFailed(Exception):
    pass


def read_ack(ack: bytes) -> tuple[str, str]:
    """Return (MSA-1 code, MSA-2 control ID) from an ACK. The producer may parse; the
    listener may not."""
    message = hl7.parse(ack.decode("latin-1"))
    msa = message.segment("MSA")
    return str(msa[1]), str(msa[2])


class MllpSender:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.resends = 0  # total extra attempts across all messages
        self._reader = None
        self._writer = None

    async def send(self, raw: bytes, control_id: str) -> int:
        """Send until we get AA for this exact control ID. Returns attempts used."""
        delay = RETRY_DELAY
        attempt = 0
        while True:
            attempt += 1
            try:
                if self._writer is None:
                    self._reader, self._writer = await open_hl7_connection(self.host, self.port)
                self._writer.writeblock(raw)
                await self._writer.drain()
                ack = await asyncio.wait_for(self._reader.readblock(), ACK_TIMEOUT)
                code, acked_id = read_ack(ack)
            except Exception as e:
                # Connection refused/reset, ACK timeout, connection closed before the
                # ACK, or an ACK we can't read: all mean "not acknowledged", so resend.
                problem = f"{type(e).__name__}: {e}"
            else:
                if code == "AA" and acked_id == control_id:
                    return attempt
                if code == "AR":
                    # Rejected: resending the same bytes won't help.
                    raise SendFailed(f"{control_id} rejected (AR)")
                problem = f"got {code} for {acked_id!r}"

            # Anything other than a matching AA: start again on a fresh connection, so a
            # late ACK for this attempt can't be mistaken for the next one.
            await self.close()
            self.resends += 1
            log.warning("resending %s in %.2fs (attempt %d: %s)", control_id, delay, attempt, problem)
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_RETRY_DELAY)

    async def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except OSError:
                pass
        self._reader = self._writer = None
