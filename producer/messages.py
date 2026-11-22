"""Synthetic HL7 v2.5.1 messages: admissions, updates, discharges and lab results.

Everything here is made up. MRNs start with "SYN", the facility is SYNTH_GENERAL, and
names are random combinations from short lists. No real patient data.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

FACILITY = "SYNTH_GENERAL"
SENDING_APP = "SYNTH_PRODUCER"

FIRST_NAMES = ["Alex", "Sam", "Jordan", "Riley", "Casey", "Morgan", "Taylor", "Jamie", "Avery", "Quinn"]
LAST_NAMES = ["Rivera", "Okafor", "Lindqvist", "Tanaka", "Moreau", "Novak", "Haddad", "Silva", "Kowal", "Byrne"]
WARDS = ["MED1", "MED2", "SURG1", "ICU"]

# (local code, name, unit, low, high) for lab results.
LABS = [
    ("K", "Potassium", "mmol/L", 3.5, 5.1),
    ("NA", "Sodium", "mmol/L", 135.0, 145.0),
    ("GLU", "Glucose", "mg/dL", 70.0, 110.0),
    ("CREAT", "Creatinine", "mg/dL", 0.6, 1.2),
    ("HGB", "Hemoglobin", "g/dL", 12.0, 17.0),
]


@dataclass(frozen=True)
class Message:
    control_id: str
    message_type: str  # e.g. "ADT^A01"
    raw: bytes  # exact bytes to send (segments end in \r)


@dataclass
class _Patient:
    mrn: str
    last: str
    first: str
    birth_date: str
    sex: str
    visit: str
    ward: str


class MessageGenerator:
    """Produces a plausible stream: admit, update, lab results, discharge.

    `seed` makes the content reproducible. Control IDs are `<run_id><seq>`, unique
    within a run; `run_id` keeps them unique across runs.
    """

    def __init__(
        self,
        seed: int | None = None,
        run_id: str | None = None,
        facility: str = FACILITY,
    ) -> None:
        self.rng = random.Random(seed)
        self.run_id = run_id or uuid.uuid4().hex[:8].upper()
        self.facility = facility
        self.seq = 0
        self.admitted: list[_Patient] = []
        self.next_mrn = self.rng.randrange(100000, 900000)

    def next(self) -> Message:
        roll = self.rng.random()
        if not self.admitted or (roll < 0.25 and len(self.admitted) < 50):
            return self._admit()
        if roll < 0.35:
            return self._update()
        if roll < 0.45:
            return self._discharge()
        return self._lab_result()

    def take(self, n: int) -> list[Message]:
        return [self.next() for _ in range(n)]

    # --- message types -----------------------------------------------------------

    def _admit(self) -> Message:
        self.next_mrn += 1
        patient = _Patient(
            mrn=f"SYN{self.next_mrn:06d}",
            last=self.rng.choice(LAST_NAMES),
            first=self.rng.choice(FIRST_NAMES),
            birth_date=f"{self.rng.randrange(1930, 2020)}{self.rng.randrange(1, 13):02d}{self.rng.randrange(1, 29):02d}",
            sex=self.rng.choice("MF"),
            visit=f"V{self.next_mrn:06d}{self.rng.randrange(10)}",
            ward=self.rng.choice(WARDS),
        )
        self.admitted.append(patient)
        return self._adt("A01", patient)

    def _update(self) -> Message:
        patient = self.rng.choice(self.admitted)
        patient.ward = self.rng.choice(WARDS)
        return self._adt("A08", patient)

    def _discharge(self) -> Message:
        patient = self.admitted.pop(self.rng.randrange(len(self.admitted)))
        return self._adt("A03", patient)

    def _adt(self, trigger: str, p: _Patient) -> Message:
        now = self._ts()
        control_id, msh = self._msh(f"ADT^{trigger}^ADT_A01", now)
        segments = [
            msh,
            f"EVN|{trigger}|{now}",
            self._pid(p),
            f"PV1|1|I|{p.ward}^^^{self.facility}||||1234^Example^Dana|||MED||||||||||{p.visit}",
        ]
        return Message(control_id, f"ADT^{trigger}", _encode(segments))

    def _lab_result(self) -> Message:
        p = self.rng.choice(self.admitted)
        now = self._ts()
        control_id, msh = self._msh("ORU^R01^ORU_R01", now)
        order_id = f"ORD{self.run_id}{self.seq:06d}"
        segments = [msh, self._pid(p), f"PV1|1|I|{p.ward}^^^{self.facility}||||||||||||||||{p.visit}"]
        segments.append(f"OBR|1|{order_id}|{order_id}|BMP^Basic metabolic panel^L|||{now}|||||||{now}")
        for i, (code, name, unit, low, high) in enumerate(self.rng.sample(LABS, k=3), start=1):
            value = round(self.rng.uniform(low * 0.8, high * 1.2), 1)
            flag = "L" if value < low else "H" if value > high else "N"
            segments.append(f"OBX|{i}|NM|{code}^{name}^L||{value}|{unit}|{low}-{high}|{flag}|||F|||{now}")
        return Message(control_id, "ORU^R01", _encode(segments))

    # --- segments ----------------------------------------------------------------

    def _msh(self, message_type: str, now: str) -> tuple[str, str]:
        self.seq += 1
        control_id = f"{self.run_id}{self.seq:06d}"
        msh = f"MSH|^~\\&|{SENDING_APP}|{self.facility}|CDP|CDP|{now}||{message_type}|{control_id}|P|2.5.1"
        return control_id, msh

    def _pid(self, p: _Patient) -> str:
        return f"PID|1||{p.mrn}^^^{self.facility}^MR||{p.last}^{p.first}||{p.birth_date}|{p.sex}"

    def _ts(self) -> str:
        return f"{datetime.now(timezone.utc):%Y%m%d%H%M%S}+0000"


def _encode(segments: list[str]) -> bytes:
    # HL7 v2 segments are terminated by \r (not \n, not \r\n).
    return ("\r".join(segments) + "\r").encode("ascii")
