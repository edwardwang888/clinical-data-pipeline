"""MLLP framing: each message travels as  <VT> payload <FS><CR>  (0x0B ... 0x1C 0x0D).

python-hl7 provides the asyncio server and stream classes (`hl7.mllp`). We read frames
with our own small `read_block` / `read_frame` instead of its `readblock()`, for two
reasons. `readblock()` raises and throws the whole frame away if *any* byte arrives
before the <VT>, and some senders put a newline between frames. And we never drop bytes
silently: anything that isn't part of a usable frame is handed back to the listener,
which keeps it as evidence with the reason.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass

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


@dataclass(frozen=True)
class Block:
    """What `read_block` found next in the stream.

    `payload` is a complete frame's content, or None if there isn't one. `discarded`
    holds any bytes that aren't part of a usable frame (exactly as received), and
    `reason` says why; the listener keeps those as evidence, so nothing is dropped
    silently. Whitespace between frames is not reported: it's common and harmless.
    """

    payload: bytes | None
    discarded: bytes = b""
    reason: str = ""
    end_of_stream: bool = False  # the connection ended
    too_large: bool = False  # a frame exceeded the size limit: close the connection


async def read_block(reader: asyncio.StreamReader) -> Block:
    """Read up to the next end marker and return the frame, plus anything unusable."""
    try:
        block = await reader.readuntil(END_BLOCK)
    except asyncio.IncompleteReadError as e:
        # The connection ended. Leftover bytes are a frame that was never completed (so
        # never ACKed; the sender resends it if it was real).
        leftover = e.partial if e.partial.strip() else b""
        return Block(None, leftover, "connection closed mid-frame" if leftover else "", end_of_stream=True)
    except asyncio.LimitOverrunError as e:
        # The frame is too big to buffer. Hand back what was received so far (it stays in
        # the reader's buffer after the error); the connection can't be resynced.
        head = await reader.read(max(e.consumed, 1))
        reason = f"frame exceeded the size limit; first {len(head)} bytes kept"
        return Block(None, head, reason, too_large=True)

    # The frame starts at the *first* <VT>; only bytes before it are noise. A later
    # <VT> stays in the payload: it may be a stray byte inside the message, or an
    # abandoned partial frame followed by a new one (see split_abandoned). Either way
    # we keep every byte, because cutting there would store a fragment and ACK it.
    start = block.find(START_BLOCK)
    if start == -1:
        return Block(None, block, "block ended with <FS><CR> but had no <VT> start byte")
    noise = block[:start]
    if noise.strip():
        return Block(block[start + 1 : -len(END_BLOCK)], noise, "bytes before the frame start")
    return Block(block[start + 1 : -len(END_BLOCK)])


async def read_frame(reader: asyncio.StreamReader) -> bytes | None:
    """Read the next frame and return just its payload, as the exact bytes received.

    A simpler view of `read_block` for code that only wants frames (e.g. a client
    reading ACKs). Returns None when the connection ends. Raises `FramingError` for a
    block with no <VT> (the stream stays in sync, so the caller can keep reading) and
    `FrameTooLarge` when a frame exceeds the reader's limit. Discarded bytes are logged.
    """
    block = await read_block(reader)
    if block.discarded:
        log.warning("%s: %d bytes not used as a frame", block.reason, len(block.discarded))
    if block.too_large:
        raise FrameTooLarge(block.reason)
    if block.payload is None and not block.end_of_stream:
        raise FramingError(block.reason)
    return block.payload


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
