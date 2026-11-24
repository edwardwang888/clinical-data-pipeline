import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest

from pipeline.raw_store import LocalRawStore, _write_create_only, raw_key, safe_segment

RECEIVED_AT = datetime(2026, 11, 20, 22, 50, 47, 123456, tzinfo=timezone.utc)
SHA = "abcdef0123456789" + "0" * 48

# Segments end in \r, and \xe9 is "é" in Latin-1 but not valid UTF-8: both must
# survive byte for byte.
RAW = b"MSH|^~\\&|SIM|SYNTH_GENERAL|CDP|CDP|20261120225047||ADT^A01|C1|P|2.5.1\rPID|||SYN000001||Ren\xe9e^Test\r"


def meta(**overrides):
    return {
        "received_at": RECEIVED_AT,
        "hospital": "SYNTH_GENERAL",
        "feed": "ADT",
        "control_id": "C1",
        **overrides,
    }


# --- file naming -------------------------------------------------------------------


def test_key_layout():
    assert raw_key("SYNTH_GENERAL", "ADT", RECEIVED_AT, SHA) == (
        "hospital=synth_general/feed=adt/date=2026-11-20/"
        "20261120T225047123456Z_abcdef012345.hl7"
    )


def test_key_suffix_for_later_attempts():
    assert raw_key("h", "adt", RECEIVED_AT, SHA, attempt=2).endswith("Z_abcdef012345_2.hl7")


def test_key_date_and_timestamp_are_utc():
    # 20:00 on Nov 19 in UTC-8 is 04:00 on Nov 20 in UTC.
    pacific = datetime(2026, 11, 19, 20, 0, tzinfo=timezone(timedelta(hours=-8)))
    key = raw_key("h", "adt", pacific, SHA)
    assert "/date=2026-11-20/20261120T040000000000Z_" in key


def test_key_rejects_naive_datetime():
    with pytest.raises(ValueError):
        raw_key("h", "adt", datetime(2026, 11, 20), SHA)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("SYNTH_GENERAL", "synth_general"),
        ("Synth General Hospital", "synth_general_hospital"),
        ("../../etc/passwd", "etc_passwd"),
        ("", "unknown"),
        (None, "unknown"),
        ("...", "unknown"),
    ],
)
def test_safe_segment(value, expected):
    assert safe_segment(value) == expected


# --- saving ------------------------------------------------------------------------


def test_save_keeps_exact_bytes_and_writes_metadata(tmp_path):
    store = LocalRawStore(tmp_path)
    saved = store.save_raw(RAW, meta())

    hl7_path = tmp_path / saved.key
    assert hl7_path.read_bytes() == RAW
    assert store.load_raw(saved.key) == RAW

    sidecar = json.loads(hl7_path.with_suffix(".json").read_text())
    assert sidecar["sha256"] == saved.sha256
    assert sidecar["size_bytes"] == len(RAW)
    assert sidecar["control_id"] == "C1"
    assert sidecar["received_at"] == "2026-11-20T22:50:47.123456+00:00"
    assert sidecar["raw_key"] == saved.key


def test_save_leaves_no_temp_files(tmp_path):
    LocalRawStore(tmp_path).save_raw(RAW, meta())
    assert not [p for p in tmp_path.rglob("*") if p.name.startswith(".tmp-")]


def test_save_fsyncs_each_file_before_naming_it_and_each_directory_before_returning(tmp_path):
    # Crash tests can't catch a missing fsync: a killed process loses nothing that the OS
    # already has in its cache. So record the calls, identifying files and directories by
    # inode (a temp file and its final name share one), and check their order.
    calls = []
    real_fsync, real_link = os.fsync, os.link

    def fsync(fd):
        calls.append(("fsync", os.fstat(fd).st_ino))
        real_fsync(fd)

    def link(src, dst):
        calls.append(("link", os.stat(src).st_ino))
        real_link(src, dst)

    with mock.patch("os.fsync", side_effect=fsync), mock.patch("os.link", side_effect=link):
        saved = LocalRawStore(tmp_path).save_raw(RAW, meta())

    hl7_path = tmp_path / saved.key
    files = [hl7_path, hl7_path.with_suffix(".json")]
    for f in files:  # each file's data is on disk before its name appears
        inode = f.stat().st_ino
        assert calls.index(("fsync", inode)) < calls.index(("link", inode)), f.name
    # The new names themselves are made durable before save_raw returns...
    last_link = max(calls.index(("link", f.stat().st_ino)) for f in files)
    assert ("fsync", hl7_path.parent.stat().st_ino) in calls[last_link:]
    # ...and so is each new directory, in its parent (hospital=, feed=, date=).
    new_dirs = [hl7_path.parent, hl7_path.parent.parent, hl7_path.parent.parent.parent]
    for d in new_dirs:
        assert ("fsync", d.parent.stat().st_ino) in calls, d.name


def test_same_message_twice_becomes_two_files(tmp_path):
    """A resent message (same bytes, even the same microsecond) is a new file."""
    store = LocalRawStore(tmp_path)
    first = store.save_raw(RAW, meta())
    second = store.save_raw(RAW, meta())

    assert first.key != second.key
    assert second.key.endswith("_1.hl7")
    assert store.load_raw(first.key) == RAW
    assert store.load_raw(second.key) == RAW
    assert len(list(tmp_path.rglob("*.hl7"))) == 2
    assert len(list(tmp_path.rglob("*.json"))) == 2


def test_save_never_overwrites_an_existing_file(tmp_path):
    # Put something else at the exact name the save will want first.
    wanted = tmp_path / raw_key("SYNTH_GENERAL", "ADT", RECEIVED_AT, hashlib.sha256(RAW).hexdigest())
    wanted.parent.mkdir(parents=True)
    wanted.write_bytes(b"original")

    saved = LocalRawStore(tmp_path).save_raw(RAW, meta())

    assert tmp_path / saved.key != wanted
    assert wanted.read_bytes() == b"original"
    assert (tmp_path / saved.key).read_bytes() == RAW


def test_write_create_only_refuses_existing_file(tmp_path):
    target = tmp_path / "a.hl7"
    target.write_bytes(b"original")
    with pytest.raises(FileExistsError):
        _write_create_only(target, b"new")
    assert target.read_bytes() == b"original"
    assert [p.name for p in tmp_path.iterdir()] == ["a.hl7"]  # temp file cleaned up


def test_load_raw_rejects_keys_outside_the_archive(tmp_path):
    store = LocalRawStore(tmp_path / "raw")
    with pytest.raises(ValueError):
        store.load_raw("../secret.txt")
