import asyncio

import pytest

from services.listener.framing import read_block, split_abandoned

VT, FS_CR = b"\x0b", b"\x1c\r"
# Segments end in \r; \xe9 is not valid UTF-8. Both must come through unchanged.
MSG_A = b"MSH|^~\\&|SIM|SYNTH_GENERAL|||20261120||ADT^A01|A1|P|2.5.1\rPID|||SYN1||Ren\xe9e\r"
MSG_B = b"MSH|^~\\&|SIM|SYNTH_GENERAL|||20261120||ORU^R01|B1|P|2.5.1\rOBX|1|NM|K||4.1\r"


def frame(payload: bytes) -> bytes:
    return VT + payload + FS_CR


def read_blocks(chunks: list[bytes], limit: int = 2**16) -> list:
    """Feed `chunks` into a stream (then EOF) and return every Block until the stream ends."""

    async def run():
        reader = asyncio.StreamReader(limit=limit)
        for chunk in chunks:
            reader.feed_data(chunk)
        reader.feed_eof()
        found = []
        while True:
            block = await read_block(reader)
            found.append(block)
            if block.end_of_stream or block.too_large:
                return found

    return asyncio.run(run())


def payloads(chunks: list[bytes]) -> list[bytes]:
    """Just the complete frames' payloads, as the listener would process them."""
    return [b.payload for b in read_blocks(chunks) if b.payload is not None]


def test_single_frame_returns_exact_payload():
    assert payloads([frame(MSG_A)]) == [MSG_A]


def test_two_frames_in_one_chunk():
    assert payloads([frame(MSG_A) + frame(MSG_B)]) == [MSG_A, MSG_B]


def test_frame_split_into_one_byte_chunks():
    data = frame(MSG_A) + frame(MSG_B)
    assert payloads([data[i : i + 1] for i in range(len(data))]) == [MSG_A, MSG_B]


def test_end_marker_split_across_chunks():
    data = frame(MSG_A)
    assert payloads([data[:-1], data[-1:]]) == [MSG_A]


def test_payload_containing_lone_fs_byte_is_not_cut_short():
    # 0x1C alone is not the end marker; only 0x1C followed by 0x0D is.
    payload = b"MSH|^~\\&|X\rNTE|1||a\x1cb\r"
    assert payloads([frame(payload)]) == [payload]


def test_newline_between_frames_is_tolerated():
    # Some senders write "\n" after each frame. python-hl7's readblock() would drop
    # the next message; we must not.
    assert payloads([frame(MSG_A) + b"\n" + frame(MSG_B) + b"\n"]) == [MSG_A, MSG_B]


def test_vt_byte_inside_payload_is_kept():
    # MLLP content shouldn't contain <VT>, but if it does (e.g. a stray control
    # character in free text) we must keep every byte. Cutting at that <VT> would
    # store only the tail and still ACK it.
    payload = b"MSH|^~\\&|A|SYNTH_GENERAL|||20261120||ORU^R01|C1|P|2.5.1\rOBX|1|TX|NOTE||line one\x0bline two\r"
    assert payloads([frame(payload)]) == [payload]


def test_abandoned_partial_frame_is_kept_with_the_next_frame():
    # The sender started a frame, gave up without an end marker, then sent a full one.
    # Both land in one block, and read_block keeps every byte of it. The listener then
    # separates the abandoned part from the real message with split_abandoned (below).
    data = VT + b"MSH|partial" + frame(MSG_B)
    assert payloads([data]) == [b"MSH|partial" + VT + MSG_B]


# --- splitting off abandoned frames ------------------------------------------------


def test_split_leaves_a_normal_message_alone():
    assert split_abandoned(MSG_A) == (b"", MSG_A)


def test_split_leaves_a_vt_inside_the_message_alone():
    # A <VT> not followed by MSH is just a stray byte in the content: keep it.
    payload = MSG_B + b"OBX|2|TX|NOTE||line one\x0bline two\r"
    assert split_abandoned(payload) == (b"", payload)


@pytest.mark.parametrize(
    "abandoned",
    [b"MSH|partial", b"PING\r\n", MSG_A[:30], MSG_A + b"PID|||SY"],
    ids=["partial-msh", "noise", "cut-inside-msh", "cut-after-msh"],
)
def test_split_separates_an_abandoned_frame_from_the_new_message(abandoned):
    assert split_abandoned(abandoned + VT + MSG_B) == (abandoned, MSG_B)


def test_split_keeps_everything_before_the_last_restart():
    # Two abandoned attempts, then the real message.
    payload = b"MSH|first" + VT + b"MSH|second" + VT + MSG_B
    assert split_abandoned(payload) == (b"MSH|first" + VT + b"MSH|second", MSG_B)


def test_split_allows_whitespace_or_bom_before_the_new_msh():
    for gap in (b"\r\n", b" ", b"\xef\xbb\xbf"):
        assert split_abandoned(b"MSH|partial" + VT + gap + MSG_B) == (b"MSH|partial", gap + MSG_B)


def test_python_hl7_writer_frames_payload():
    """The ACK is sent with python-hl7's writeblock; check what it puts on the wire."""
    from hl7.mllp import MLLPStreamWriter

    class FakeTransport:
        def __init__(self):
            self.data = b""

        def write(self, data):
            self.data += data

        def is_closing(self):
            return False

        def close(self):
            pass

    transport = FakeTransport()

    async def run():
        writer = MLLPStreamWriter(transport, protocol=None, reader=None, loop=asyncio.get_running_loop())
        writer.writeblock(MSG_A)

    asyncio.run(run())
    assert transport.data == VT + MSG_A + FS_CR


# --- read_block: nothing is dropped silently -----------------------------------------


def test_block_reports_noise_before_the_frame():
    b, _end = read_blocks([b"garbage" + frame(MSG_A)])
    assert b.payload == MSG_A
    assert b.discarded == b"garbage"
    assert "before the frame start" in b.reason
    assert b.kind == "noise_before_frame"


def test_block_ignores_whitespace_between_frames():
    # A newline after each frame is common and harmless: not bad data, nothing to keep.
    b, _end = read_blocks([b"\r\n" + frame(MSG_A)])
    assert (b.payload, b.discarded) == (MSG_A, b"")


def test_block_without_start_byte_is_handed_back_whole():
    data = b"MSH|no start byte" + FS_CR
    first, second, _end = read_blocks([data + frame(MSG_A)])
    assert first.payload is None
    assert first.discarded == data  # exact bytes as received, end marker included
    assert "no <VT>" in first.reason
    assert first.kind == "no_start_byte"
    assert second.payload == MSG_A  # and the stream is still in sync


def test_block_cut_off_by_disconnect_is_handed_back():
    cut = VT + b"MSH|cut off"
    [b] = read_blocks([cut])
    assert b.payload is None and b.end_of_stream
    assert b.discarded == cut
    assert "closed mid-frame" in b.reason
    assert b.kind == "cut_off_by_disconnect"


def test_clean_disconnect_has_nothing_to_keep():
    for tail in (b"", b"\r\n"):
        [b] = read_blocks([tail])
        assert (b.payload, b.discarded, b.end_of_stream) == (None, b"", True)


def test_oversized_frame_hands_back_what_was_received():
    [b] = read_blocks([VT + b"x" * 500 + FS_CR], limit=100)
    assert b.payload is None and b.too_large
    assert b.discarded.startswith(VT + b"xxx")
    assert "size limit" in b.reason
    assert b.kind == "too_large"

