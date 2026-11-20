"""Raw archive: the exact bytes of every message we received, kept forever.

`RawStore` is the interface the rest of the pipeline depends on. `LocalRawStore`
writes to a folder on disk; a GCS or S3 store can implement the same methods later
without touching the listener.

Layout (relative "keys", the same shape an object store would use):

    hospital=<id>/feed=<type>/date=<YYYY-MM-DD>/<received_ts>_<sha256[:12]>.hl7
    hospital=<id>/feed=<type>/date=<YYYY-MM-DD>/<received_ts>_<sha256[:12]>.json

Rules this module enforces:
- Bytes in, bytes out. Nothing is decoded, re-encoded or normalized (HL7 segments end
  in `\\r`, and the character set varies per sender).
- Create-only. A file is never overwritten or edited. If a name is already taken, the
  next free name is used instead, so a resent message simply becomes another file.
- Durable before returning. Data is fsynced before `save_raw` returns, because the
  listener ACKs the sender as soon as it does.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

# Same bytes received in the same microsecond get suffixes _1, _2, ... This bound only
# stops a runaway loop; in practice the first or second name is free.
_MAX_NAME_ATTEMPTS = 1000


@dataclass(frozen=True)
class SavedRaw:
    """Where a message was stored, and what we know about it."""

    key: str  # path of the .hl7 file, relative to the archive root
    sha256: str  # hex digest of the exact raw bytes; the message's lineage id
    meta: dict[str, Any]  # everything written to the .json file next to it


class RawStore(Protocol):
    def save_raw(self, raw: bytes, meta: dict[str, Any]) -> SavedRaw:
        """Store `raw` durably and create-only. `meta` must contain `received_at`
        (timezone-aware datetime), `hospital` and `feed`; other keys are kept as-is
        in the metadata file."""
        ...

    def load_raw(self, key: str) -> bytes:
        """Return the exact bytes stored under `key`."""
        ...


def safe_segment(value: str | None) -> str:
    """Make a value safe to use as one path segment: lowercase, only [a-z0-9_-].

    Hospital and feed come from the message header, which we don't trust: a value like
    "../../etc" must never escape the archive folder.
    """
    cleaned = re.sub(r"[^a-z0-9_-]+", "_", (value or "").strip().lower()).strip("_")
    return cleaned[:64] or "unknown"


def raw_key(hospital: str, feed: str, received_at: datetime, sha256: str, attempt: int = 0) -> str:
    """Build the archive key for a message. Dates and timestamps are in UTC, from the
    time *we* received the message (the message's own timestamp is unparsed and
    untrusted at this point)."""
    if received_at.tzinfo is None:
        raise ValueError("received_at must be timezone-aware")
    ts = received_at.astimezone(timezone.utc)
    name = f"{ts:%Y%m%dT%H%M%S%f}Z_{sha256[:12]}"
    if attempt:
        name += f"_{attempt}"
    return (
        f"hospital={safe_segment(hospital)}/feed={safe_segment(feed)}"
        f"/date={ts:%Y-%m-%d}/{name}.hl7"
    )


class LocalRawStore:
    """RawStore backed by a local folder."""

    def __init__(self, root: str | os.PathLike[str]) -> None:
        self.root = Path(root)

    def save_raw(self, raw: bytes, meta: dict[str, Any]) -> SavedRaw:
        sha256 = hashlib.sha256(raw).hexdigest()

        for attempt in range(_MAX_NAME_ATTEMPTS):
            key = raw_key(meta["hospital"], meta["feed"], meta["received_at"], sha256, attempt)
            path = self.root / key
            _make_dirs(path.parent)
            try:
                _write_create_only(path, raw)
                break
            except FileExistsError:
                continue  # name taken (same bytes, same microsecond): try the next one
        else:
            raise RuntimeError(f"no free file name for message {sha256[:12]}")

        record = {k: _jsonable(v) for k, v in meta.items()}
        record.update(sha256=sha256, size_bytes=len(raw), raw_key=key)
        _write_create_only(
            path.with_suffix(".json"),
            json.dumps(record, indent=2, sort_keys=True).encode("utf-8"),
        )
        # Make the new directory entries themselves durable, not just the file contents.
        _fsync_dir(path.parent)
        return SavedRaw(key=key, sha256=sha256, meta=record)

    def load_raw(self, key: str) -> bytes:
        return self._path_for(key).read_bytes()

    def _path_for(self, key: str) -> Path:
        root = self.root.resolve()
        path = (root / key).resolve()
        if not path.is_relative_to(root):
            raise ValueError(f"key escapes the archive root: {key!r}")
        return path


def _write_create_only(path: Path, data: bytes) -> None:
    """Create `path` containing `data`. Raise FileExistsError if it already exists.

    We write to a hidden temp file, fsync it, then hard-link it to the final name.
    `os.link` refuses to replace an existing file, so nothing is ever overwritten, and
    the final name only ever points at a complete, fsynced file: a crash halfway
    through leaves at most a stray `.tmp-*` file, never a truncated message.
    """
    tmp = path.parent / f".tmp-{uuid.uuid4().hex}"
    try:
        with open(tmp, "xb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.link(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _make_dirs(directory: Path) -> None:
    """mkdir -p, fsyncing each parent so new directories survive a power loss."""
    missing = []
    d = directory
    while not d.exists():
        missing.append(d)
        d = d.parent
    for d in reversed(missing):
        try:
            d.mkdir()
        except FileExistsError:
            pass  # another connection created it at the same moment
        _fsync_dir(d.parent)


def _fsync_dir(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _jsonable(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime) else value
