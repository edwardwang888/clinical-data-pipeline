"""MLLP framing: each message travels as  <VT> payload <FS><CR>  (0x0B ... 0x1C 0x0D).

python-hl7 provides the asyncio server and stream classes (`hl7.mllp`). We read frames
with our own small `read_frame` instead of its `readblock()`, for one reason:
`readblock()` raises and throws the whole frame away if *any* byte arrives before the
<VT>. Some senders put a newline between frames, and we must not drop a message (and
leave the sender waiting for an ACK) over a stray newline.
"""

from __future__ import annotations

import asyncio
import logging
import re

log = logging.getLogger(__name__)

START_BLOCK = b"\x0b"  # <VT>
END_BLOCK = b"\x1c\x0d"  # <FS><CR>

# A <VT> that is followed (after optional whitespace or a UTF-8 BOM) by "MSH" starts a
# new message. A <VT> followed by anything else is just a stray byte in the content.
_NEW_MESSAGE_START = re.compile(rb"\x0b(?=\s*(?:\xef\xbb\xbf)?\s*MSH)")


class FramingError(Exception):
    """The byte stream doesn't contain a valid MLLP frame here."""


class FrameTooLarge(FramingError):
    """A frame exceeded the reader's size limit. The stream can't be resynced, so the
    connection must be closed."""


async def read_frame(reader: asyncio.StreamReader) -> bytes | None:
    """Read the next frame and return its payload as the exact bytes received.

    Returns None when the connection ends (cleanly, or in the middle of a frame that
    was never completed, which we never ACKed, so the sender will resend it).

    Raises `FramingError` for a block that has an end marker but no start marker (the
    stream is still in sync, so the caller can keep reading), and `FrameTooLarge` when
    a frame exceeds the reader's limit.
    """
    try:
        block = await reader.readuntil(END_BLOCK)
    except asyncio.IncompleteReadError as e:
        if e.partial.strip():
            log.warning("connection closed mid-frame; dropped %d incomplete bytes", len(e.partial))
        return None
    except asyncio.LimitOverrunError as e:
        raise FrameTooLarge(f"frame larger than the {e.consumed}+ byte limit") from e

    # The frame starts at the *first* <VT>; only bytes before it are noise. A later
    # <VT> stays in the payload: it may be a stray byte inside the message, or an
    # abandoned partial frame followed by a new one. Either way we keep every byte,
    # because cutting there would store a fragment and still ACK it as the message.
    start = block.find(START_BLOCK)
    if start == -1:
        raise FramingError(f"{len(block)} bytes ended with <FS><CR> but had no <VT> start byte")
    if start > 0:
        noise = block[:start]
        level = logging.DEBUG if not noise.strip() else logging.WARNING
        log.log(level, "skipped %d bytes before the frame start: %r", len(noise), noise[:40])
    return block[start + 1 : -len(END_BLOCK)]


def split_abandoned(payload: bytes) -> tuple[bytes, bytes]:
    """Separate frames the sender abandoned from the message that follows them.

    `read_frame` keeps every byte from the first <VT>, so that a stray <VT> inside a
    message can't cut it short. But when a later <VT> starts a new MSH, the sender gave
    up on what came before (it never sent the end marker) and started over. Return
    `(abandoned, message)`: `abandoned` is everything before the last such <VT> (b"" if
    there is none) and `message` is what follows it. The abandoned part was never
    completed, so the sender gets no reply for it and resends it if it was real.
    """
    last = None
    for last in _NEW_MESSAGE_START.finditer(payload):
        pass
    if last is None:
        return b"", payload
    return payload[: last.start()], payload[last.end() :]
