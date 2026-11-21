# AGENTS.md

## What this project is

A practice project that simulates a hospital sending HL7 v2 messages and builds the
pipeline that cleans, models and serves that data: feature tables for model training,
reporting tables, and a small read API.

The goal is learning, not production scale. Favor clear, readable code that is easy to
explain over clever or heavily abstracted code.

**Read `DESIGN.md` before starting any work.** It is the source of truth for architecture,
decisions and the build plan. If a decision changes while building, update `DESIGN.md`
in the same PR (especially the "Key decisions and trade-offs" and "Failure modes" tables).

## Rules that must never be broken

- **Synthetic data only.** Never add real patient data, real hospital names or real MRNs.
- **Never ACK before the message is durably stored.** The listener sends `AA` only after
  the raw write and the queue publish both succeed.
- **The listener does not parse.** It reads only the MSH fields it needs for the ACK and
  file metadata. If it can't read those (no MSH, control ID or message type), it keeps the
  exact bytes as evidence in the rejected area, never queues them, and replies `AR`;
  rejections, too, are sent only after the bytes are stored. Anything with a readable
  header is stored and ACKed, however wrong its contents.
- **The raw archive is append-only.** Create-only writes, unique file names, never
  overwrite or edit. Store exact bytes (segments end in `\r`; don't normalize line endings
  or decode/re-encode).
- **Consumers are idempotent and order-independent.** Delivery is at least once and
  unordered; processing the same message twice or out of order must give the same result.
- **Bad data never blocks the pipeline.** Messages with a readable header but bad contents
  go to quarantine with the error and a pointer to the raw file; they are never rejected
  at the door, so they can't stall the sender's queue.
- **Nothing is dropped silently; bad input goes to the dead-letter queue, not the logs.**
  Every unidentifiable message, and any bytes the listener can't use as a message (noise
  before a frame, a block with no start byte, a frame cut off by a disconnect or over the
  size limit, an abandoned frame), is kept in the rejected area and gets an entry in the
  `hl7.dlq` stream with its kind, reason, sha256 and a pointer to that evidence. If the
  evidence file can't be saved, the entry carries the bytes. Only if the DLQ itself is
  unreachable does a log line record the input. Whitespace between frames is the only
  thing skipped.
- **Lineage everywhere.** Every derived row keeps `raw_message_id` (sha256 of raw bytes)
  and `source_system`.

## Stack

- Python 3.11+, managed with `uv` and `pyproject.toml`
- asyncio + `python-hl7` (`hl7.mllp`) for MLLP
- Redis Streams for the queue (consumer groups, `XACK`, `XAUTOCLAIM`)
- Local folder for the raw archive (`data/raw/...`, gitignored)
- DuckDB + dbt (duckdb adapter) for staging and models
- FastAPI for the read API
- Docker Compose to run everything; a `Makefile` with `up`, `down`, `logs`, `test`
- pytest for tests

Storage and queue access go through small interfaces (`RawStore.save_raw(bytes, meta)`,
`Publisher.publish(event)`) so GCS and Pub/Sub can be swapped in later without touching
pipeline logic.

## Suggested layout

```
services/listener/     MLLP listener
services/injector/     messiness injector (MLLP proxy)
producer/              Python producer (fallback / edge cases)
pipeline/              parser, mapping, loaders
config/sites/          per-hospital mapping config (YAML)
dbt/                   staging and canonical models, features, marts
api/                   FastAPI read API
tests/                 unit and integration tests
data/                  local raw archive, DuckDB files (gitignored)
```

## How to work

- Build one milestone from the `DESIGN.md` build plan at a time, on its own branch, and
  open a PR for each. Small, focused commits.
- Each milestone needs tests, including the failure modes it touches (see the
  "Failure modes" table in `DESIGN.md`).
- Prefer the simplest thing that is correct. Add a dependency only when it clearly earns
  its place, and say why in the PR.
- If something in `DESIGN.md` turns out to be wrong or impractical, stop and explain the
  problem and options before changing direction.
- Keep the `README.md` current: what runs, how to start it, how to run tests.

## After each milestone

In the PR description and in your final message, include:

1. What was built and how to run it.
2. The key decisions made and the alternatives considered.
3. Two or three key lessons from this milestone, each tied to the concrete failure it
   prevents (for example, why the listener stores a message before ACKing it).
