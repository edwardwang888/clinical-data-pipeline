# Clinical Data Pipeline

A local practice pipeline for **synthetic** hospital data. A simulated hospital sends
HL7 v2 messages over MLLP; the pipeline stores the exact raw bytes, queues them, and
(in later milestones) parses, maps and models them into feature tables, reporting tables
and a small read API. The point is to practice the hard parts of clinical data:
duplicates, corrections, late data and replay. Scale is not the point.

**Synthetic data only.** No real patient data, hospital names or MRNs ever go in this repo.

## Docs

- [`DESIGN.md`](DESIGN.md): architecture, key decisions, failure modes and the build plan.
- [`AGENTS.md`](AGENTS.md): rules that must never be broken, the stack and how to work.

## Status

| Milestone | State |
|---|---|
| 1. Getting data in (producer, MLLP listener, raw archive, Redis queue) | Done |
| 2–9 (see `DESIGN.md`) | Not started |

## What runs

```
producer ──MLLP──▶ listener :2575 ──▶ data/raw/... (.hl7 + .json)
                        │   │
                        │   └──XADD──▶ Redis stream hl7.raw ──▶ consumer (logs + XACK)
                        │
                        └── bad input ──▶ data/rejected/... (evidence) + Redis stream hl7.dlq
```

| Service | Code | What it does |
|---|---|---|
| `producer` | `producer/` | Sends synthetic ADT and lab-result messages (2/s); resends until it gets `AA` |
| `listener` | `services/listener/` | MLLP server: saves exact bytes, publishes an event, *then* ACKs. Messages it can't identify get `AR`. Those, and any other bytes it can't use (noise, broken or cut-off frames), are kept in `data/rejected/` and recorded in the dead-letter queue `hl7.dlq` with the reason |
| `redis` | | Redis Streams queue, persisted with an fsync on every write |
| `consumer` | `pipeline/consumer.py` | Stand-in for the parser: reads `hl7.raw` with a consumer group, logs, XACKs |

Raw files land in `data/raw/` (gitignored):

```
data/raw/hospital=synth_general/feed=adt/date=2026-11-20/20261120T231725395643Z_e5ca00e2196d.hl7
data/raw/hospital=synth_general/feed=adt/date=2026-11-20/20261120T231725395643Z_e5ca00e2196d.json
```

## Run it

Needs Docker with Compose v2.

```bash
make up
```

```bash
make logs
```

```bash
make dlq
```

`make dlq` shows the latest dead-letter entries: bad input the listener received, why it
couldn't be used, and where its exact bytes are kept.

```bash
make down
```

To try a listener crash yourself: `docker compose kill listener`, then
`docker compose start listener`. The producer logs its resends, and every ACKed message
still has a raw file and an event.

## Tests

```bash
make test
```

This runs the whole suite in a container against the Compose Redis. Without Docker, use
`make test-local` (needs [uv](https://docs.astral.sh/uv/); the Redis tests also need
`redis-server` on your PATH, otherwise they are skipped).

To check the code with ruff (needs uv; the rules are in `pyproject.toml`):

```bash
make lint
```

- `tests/unit/`: MLLP framing, MSH header reading and ACKs, raw file naming and
  create-only writes, the "ACK only after save and publish" rule, and producer resends.
- `tests/integration/`: the consumer against real Redis, and the milestone test. That test
  sends 1,000 messages over 4 connections, brings the listener down partway through,
  restarts it, and checks that every ACKed message has a raw file with its exact bytes
  and a queue event. It goes down five ways: SIGKILL, SIGTERM, and three crashes at an
  exact step (after the file is saved, after the event is published, in the middle of
  writing a file), by running the listener through `tests/crashing_listener.py`, which
  patches one function to crash there. Production code has no crash hooks.

## Layout

```
services/listener/   MLLP listener (framing, MSH header reader, ACKs, server)
pipeline/            RawStore and Publisher interfaces + local/Redis implementations, consumer
producer/            synthetic message generator and MLLP sender
tests/               unit and integration tests
data/                local raw archive (gitignored)
```
