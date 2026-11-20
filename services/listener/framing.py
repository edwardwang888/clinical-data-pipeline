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

log = logging.getLogger(__name__)

START_BLOCK = b"\x0b"  # <VT>
END_BLOCK = b"\x1c\x0d"  # <FS><CR>


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

    # Use the *last* <VT>: anything before it is noise, or the start of an earlier
    # frame that was abandoned without an end marker (never ACKed, so it gets resent).
    start = block.rfind(START_BLOCK)
    if start == -1:
        raise FramingError(f"{len(block)} bytes ended with <FS><CR> but had no <VT> start byte")
    if start > 0:
        noise = block[:start]
        level = logging.DEBUG if not noise.strip() else logging.WARNING
        log.log(level, "skipped %d bytes before the frame start: %r", len(noise), noise[:40])
    return block[start + 1 : -len(END_BLOCK)]
