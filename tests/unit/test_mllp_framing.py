import asyncio

import pytest

from services.listener.framing import FramingError, FrameTooLarge, read_frame

VT, FS_CR = b"\x0b", b"\x1c\r"
# Segments end in \r; \xe9 is not valid UTF-8. Both must come through unchanged.
MSG_A = b"MSH|^~\\&|SIM|SYNTH_GENERAL|||20261120||ADT^A01|A1|P|2.5.1\rPID|||SYN1||Ren\xe9e\r"
MSG_B = b"MSH|^~\\&|SIM|SYNTH_GENERAL|||20261120||ORU^R01|B1|P|2.5.1\rOBX|1|NM|K||4.1\r"


def frame(payload: bytes) -> bytes:
    return VT + payload + FS_CR


def read_all(chunks: list[bytes], limit: int = 2**16) -> list[bytes | None | Exception]:
    """Feed `chunks` into a stream (then EOF) and read frames until the stream ends."""

    async def run():
        reader = asyncio.StreamReader(limit=limit)
        for chunk in chunks:
            reader.feed_data(chunk)
        reader.feed_eof()
        results = []
        while True:
            try:
                payload = await read_frame(reader)
            except FrameTooLarge as e:
                results.append(e)
                return results
            except FramingError as e:
                results.append(e)
                continue
            results.append(payload)
            if payload is None:
                return results

    return asyncio.run(run())


def test_single_frame_returns_exact_payload():
    assert read_all([frame(MSG_A)]) == [MSG_A, None]


def test_two_frames_in_one_chunk():
    assert read_all([frame(MSG_A) + frame(MSG_B)]) == [MSG_A, MSG_B, None]


def test_frame_split_into_one_byte_chunks():
    data = frame(MSG_A) + frame(MSG_B)
    assert read_all([data[i : i + 1] for i in range(len(data))]) == [MSG_A, MSG_B, None]


def test_end_marker_split_across_chunks():
    data = frame(MSG_A)
    assert read_all([data[:-1], data[-1:]]) == [MSG_A, None]


def test_payload_containing_lone_fs_byte_is_not_cut_short():
    # 0x1C alone is not the end marker; only 0x1C followed by 0x0D is.
    payload = b"MSH|^~\\&|X\rNTE|1||a\x1cb\r"
    assert read_all([frame(payload)]) == [payload, None]


def test_newline_between_frames_is_tolerated():
    # Some senders write "\n" after each frame. python-hl7's readblock() would drop
    # the next message; we must not.
    assert read_all([frame(MSG_A) + b"\n" + frame(MSG_B) + b"\n"]) == [MSG_A, MSG_B, None]


def test_vt_byte_inside_payload_is_kept():
    # MLLP content shouldn't contain <VT>, but if it does (e.g. a stray control
    # character in free text) we must keep every byte. Cutting at that <VT> would
    # store only the tail and still ACK it.
    payload = b"MSH|^~\\&|A|SYNTH_GENERAL|||20261120||ORU^R01|C1|P|2.5.1\rOBX|1|TX|NOTE||line one\x0bline two\r"
    assert read_all([frame(payload)]) == [payload, None]


def test_abandoned_partial_frame_is_kept_with_the_next_frame():
    # The sender started a frame, gave up without an end marker, then sent a full one.
    # Both land in one block. We keep all of it (nothing is ever thrown away); the
    # parser will quarantine it, and the sender resends its real message because the
    # ACK echoes the partial frame's control ID, not the one it is waiting for.
    data = VT + b"MSH|partial" + frame(MSG_B)
    assert read_all([data]) == [b"MSH|partial" + VT + MSG_B, None]


def test_block_without_start_byte_is_an_error_but_stream_stays_usable():
    results = read_all([b"garbage" + FS_CR + frame(MSG_A)])
    assert isinstance(results[0], FramingError)
    assert results[1:] == [MSG_A, None]


def test_clean_close_between_frames_returns_none():
    assert read_all([]) == [None]


def test_close_mid_frame_returns_none_without_a_message():
    # Never completed, so never ACKed: the sender will resend it.
    assert read_all([frame(MSG_A)[:-5]]) == [None]


def test_frame_over_the_limit_raises():
    results = read_all([frame(b"x" * 500)], limit=100)
    assert isinstance(results[0], FrameTooLarge)


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


@pytest.mark.parametrize("noise", [b"\r\n", b"   ", b"\x00\x00"])
def test_any_noise_before_start_byte_is_dropped(noise):
    assert read_all([noise + frame(MSG_A)]) == [MSG_A, None]
