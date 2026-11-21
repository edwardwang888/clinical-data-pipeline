"""Build HL7 v2 ACK messages (original acknowledgment mode).

    AA  Application Accept: we stored the message durably and queued it.
    AE  Application Error: we could not store it (or its evidence). The sender should resend.
    AR  Application Reject: the header is unreadable, so we kept the bytes as evidence.
        Resending the same bytes won't help.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from services.listener.msh import MshHeader

LISTENER_APP = "CDP_LISTENER"
LISTENER_FACILITY = "CDP"
DEFAULT_VERSION = "2.5.1"


def build_ack(
    header: MshHeader,
    code: str,
    *,
    text: str = "",
    now: datetime | None = None,
    ack_control_id: str | None = None,
) -> bytes:
    """Return the ACK for the message whose header is `header`, as bytes (unframed).

    Sender and receiver are swapped relative to the original, and MSA-2 echoes the
    original control ID so the sender knows which message this answers. The sender's
    own separators are reused so the ACK reads naturally to it.
    """
    if code not in ("AA", "AE", "AR"):
        raise ValueError(f"unknown acknowledgment code {code!r}")
    s = header.field_sep
    c = header.component_sep
    now = now or datetime.now(timezone.utc)
    trigger = header.trigger_event

    msh = s.join(
        [
            "MSH",
            header.encoding_chars,
            header.receiving_app or LISTENER_APP,  # MSH-3: we are now the sender
            header.receiving_facility or LISTENER_FACILITY,  # MSH-4
            header.sending_app,  # MSH-5: reply to whoever sent the original
            header.sending_facility,  # MSH-6
            f"{now.astimezone(timezone.utc):%Y%m%d%H%M%S}+0000",  # MSH-7
            "",  # MSH-8 security
            f"ACK{c}{trigger}{c}ACK" if trigger else "ACK",  # MSH-9
            ack_control_id or uuid.uuid4().hex[:20],  # MSH-10: the ACK's own id
            header.processing_id or "P",  # MSH-11
            header.version or DEFAULT_VERSION,  # MSH-12
        ]
    )
    msa = ["MSA", code, header.control_id]
    if text:
        msa.append(_strip_separators(text, header))  # MSA-3 text message
    segments = [msh, s.join(msa)]
    return ("\r".join(segments) + "\r").encode("latin-1", errors="replace")


def _strip_separators(text: str, header: MshHeader) -> str:
    """Free text must not contain the sender's separators, or it would break the ACK."""
    for ch in header.field_sep + header.encoding_chars + "\r\n":
        text = text.replace(ch, " ")
    return text
