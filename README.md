# Clinical Data Pipeline

A local practice pipeline for **synthetic** hospital data. A simulated hospital sends
HL7 v2 messages over MLLP; the pipeline stores the exact raw bytes, queues them, and
parses, maps and models them into feature tables, reporting tables and a small read API.
The point is to practice the hard parts of clinical data — duplicates, corrections, late
data and replay — not scale.

**Synthetic data only.** No real patient data, hospital names or MRNs ever go in this repo.

## Docs

- [`DESIGN.md`](DESIGN.md) — architecture, key decisions, failure modes and the build plan.
- [`AGENTS.md`](AGENTS.md) — rules that must never be broken, stack and how to work.

## Status

| Milestone | State |
|---|---|
| 1. Getting data in (MLLP listener, raw archive, Redis queue) | In progress |
| 2–9 (see `DESIGN.md`) | Not started |

How to run it and run the tests will be added here as each milestone lands.
