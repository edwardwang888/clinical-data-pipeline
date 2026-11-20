"""Read the handful of MSH (header) fields the listener needs. This is NOT a parser.

The listener needs a few header fields to name the raw file and to build the ACK:
sending facility (-> hospital), message type (-> feed), control ID and version.
Everything else waits for the real parser downstream.

`read_msh` never raises. If the header can't be read, it returns defaults with
`error` set, and the listener still stores and ACKs the message: a message we can't
read must never block the feed. It gets quarantined later, at parse time.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace

DEFAULT_ENCODING_CHARS = "^~\\&"


@dataclass(frozen=True)
class MshHeader:
    field_sep: str = "|"
    encoding_chars: str = DEFAULT_ENCODING_CHARS
    sending_app: str = ""  # MSH-3
    sending_facility: str = ""  # MSH-4
    receiving_app: str = ""  # MSH-5
    receiving_facility: str = ""  # MSH-6
    message_type: str = ""  # MSH-9, as sent, e.g. "ADT^A01^ADT_A01"
    control_id: str = ""  # MSH-10
    processing_id: str = ""  # MSH-11
    version: str = ""  # MSH-12
    error: str | None = None  # why the header couldn't be read, if it couldn't

    @property
    def component_sep(self) -> str:
        return self.encoding_chars[0] if self.encoding_chars else "^"

    def _component(self, value: str, index: int) -> str:
        parts = value.split(self.component_sep)
        return parts[index] if index < len(parts) else ""

    @property
    def message_code(self) -> str:
        """e.g. "ADT" from "ADT^A01^ADT_A01"."""
        return self._component(self.message_type, 0)

    @property
    def trigger_event(self) -> str:
        """e.g. "A01" from "ADT^A01^ADT_A01"."""
        return self._component(self.message_type, 1)

    @property
    def facility_id(self) -> str:
        """First component of MSH-4 (the namespace id), e.g. "SYNTH_GENERAL"."""
        return self._component(self.sending_facility, 0)


def read_msh(raw: bytes) -> MshHeader:
    """Read the MSH fields we need from the first segment of `raw`. Never raises."""
    try:
        return _read_msh(raw)
    except Exception as e:  # defensive: reading the header must never stop a save
        return MshHeader(error=f"unexpected error reading MSH: {e!r}")


def _read_msh(raw: bytes) -> MshHeader:
    # Only look at the first segment. Segments should end in \r; tolerate \n too.
    first_segment = re.split(rb"[\r\n]", raw.lstrip(), maxsplit=1)[0]
    # Latin-1 maps every byte to exactly one character, so decoding can't fail, and
    # encoding the ACK back to Latin-1 returns the sender's exact bytes (e.g. for the
    # control ID). The raw message itself is never decoded; this is only for metadata.
    text = first_segment.decode("latin-1")

    if not text.startswith("MSH") or len(text) < 8:
        return MshHeader(error="message does not start with an MSH segment")
    sep = text[3]
    if sep.isalnum() or sep.isspace():
        return MshHeader(error=f"invalid field separator {sep!r}")

    fields = text.split(sep)

    def field(n: int) -> str:
        # fields[0] is "MSH" and MSH-1 is the separator itself, so MSH-n is fields[n - 1].
        return fields[n - 1] if n - 1 < len(fields) else ""

    header = MshHeader(
        field_sep=sep,
        encoding_chars=field(2) or DEFAULT_ENCODING_CHARS,
        sending_app=field(3),
        sending_facility=field(4),
        receiving_app=field(5),
        receiving_facility=field(6),
        message_type=field(9),
        control_id=field(10),
        processing_id=field(11),
        version=field(12),
    )
    if not header.control_id:
        return replace(header, error="MSH-10 (message control ID) is empty")
    if not header.message_type:
        return replace(header, error="MSH-9 (message type) is empty")
    return header
