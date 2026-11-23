import random
from datetime import datetime, timezone
from unittest import mock

import pytest

from services.listener.ack import build_ack
from services.listener.msh import MshHeader, read_msh

ADT = (
    b"MSH|^~\\&|SIMHOSP|SYNTH_GENERAL^1.2.3^ISO|CDP|CDP_FAC|20261120225047||ADT^A01^ADT_A01"
    b"|CTRL0001|P|2.5.1\rEVN|A01|20261120225047\rPID|||SYN000001\r"
)
NOW = datetime(2026, 11, 20, 23, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def fixed_ack_time_and_id():
    """Fix the ACK's timestamp (MSH-7) and its own control ID (MSH-10), so whole ACKs can
    be compared."""
    with (
        mock.patch("services.listener.ack.datetime") as clock,
        mock.patch("services.listener.ack.uuid") as ids,
    ):
        clock.now.return_value = NOW
        ids.uuid4.return_value.hex = "ACK1"
        yield


def test_reads_the_fields_the_listener_needs():
    h = read_msh(ADT)
    assert h.error is None
    assert h.sending_app == "SIMHOSP"
    assert h.sending_facility == "SYNTH_GENERAL^1.2.3^ISO"
    assert h.facility_id == "SYNTH_GENERAL"
    assert h.receiving_app == "CDP"
    assert h.receiving_facility == "CDP_FAC"
    assert h.message_type == "ADT^A01^ADT_A01"
    assert h.message_code == "ADT"
    assert h.trigger_event == "A01"
    assert h.control_id == "CTRL0001"
    assert h.processing_id == "P"
    assert h.version == "2.5.1"


def test_tolerates_newline_segment_terminators_and_leading_whitespace():
    h = read_msh(b"\n" + ADT.replace(b"\r", b"\n"))
    assert h.error is None
    assert h.control_id == "CTRL0001"


def test_utf8_byte_order_mark_before_msh_is_skipped():
    h = read_msh(b"\xef\xbb\xbf" + ADT)
    assert h.error is None
    assert h.control_id == "CTRL0001"


def test_short_header_reads_what_is_there():
    h = read_msh(b"MSH|^~\\&|APP|FAC")
    assert h.facility_id == "FAC"
    assert h.control_id == ""
    assert h.error == "MSH-10 (message control ID) is empty"


@pytest.mark.parametrize(
    "raw",
    [b"", b"hello world", b"PID|||123\r", b"MSH", b"MSHA^~\\&|x|y", b"\x00\xff\xfe" * 10],
)
def test_unreadable_header_returns_defaults_with_error(raw):
    h = read_msh(raw)
    assert h.error
    assert h.control_id == ""
    assert h.field_sep == "|"


def test_never_raises_on_random_bytes():
    rng = random.Random(42)
    for _ in range(2000):
        raw = bytes(rng.randrange(256) for _ in range(rng.randrange(0, 80)))
        if rng.random() < 0.5:
            raw = b"MSH" + raw
        read_msh(raw)  # must not raise


def test_control_id_bytes_round_trip_into_the_ack():
    # A non-ASCII control ID must come back in MSA-2 byte for byte.
    raw = ADT.replace(b"CTRL0001", b"CTRL\xe90001")
    ack = build_ack(read_msh(raw), "AA")
    assert b"\rMSA|AA|CTRL\xe90001\r" in ack


@pytest.mark.usefixtures("fixed_ack_time_and_id")
def test_aa_ack_shape():
    ack = build_ack(read_msh(ADT), "AA")
    assert ack == (
        b"MSH|^~\\&|CDP|CDP_FAC|SIMHOSP|SYNTH_GENERAL^1.2.3^ISO|20261120230000+0000||ACK^A01^ACK"
        b"|ACK1|P|2.5.1\r"
        b"MSA|AA|CTRL0001\r"
    )


def test_ae_ack_carries_text_without_breaking_fields():
    ack = build_ack(read_msh(ADT), "AE", text="disk full | retry ^ later")
    msa = ack.split(b"\r")[1]
    assert msa == b"MSA|AE|CTRL0001|disk full   retry   later"


def test_reject_for_unreadable_message_is_still_valid_hl7():
    ack = build_ack(read_msh(b"not hl7 at all"), "AR", text="rejected: no MSH")
    assert ack.startswith(b"MSH|^~\\&|CDP_LISTENER|CDP|||")
    assert ack.endswith(b"\rMSA|AR||rejected: no MSH\r")


@pytest.mark.usefixtures("fixed_ack_time_and_id")
def test_ack_uses_senders_separators():
    raw = b"MSH#$%\\&#APP#FAC#R#RF#20261120##ORU$R01#C9#P#2.4\r"
    ack = build_ack(read_msh(raw), "AA")
    assert ack.startswith(b"MSH#$%\\&#R#RF#APP#FAC#20261120230000+0000#")
    assert b"#ACK$R01$ACK#ACK1#P#2.4\r" in ack
    assert ack.endswith(b"MSA#AA#C9\r")


def test_rejects_unknown_ack_code():
    with pytest.raises(ValueError):
        build_ack(MshHeader(), "OK")
