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

## Code style

- **Readable over clever.** Write code a learner can follow top to bottom. When a
  simplification keeps the behaviour identical (the tests prove it), take it.
- **No `global` or `nonlocal`.** Keep changing state in the object that owns it: a small
  class, or a `threading.Event` for a stop flag. Module-level names are for constants,
  loggers and other values that never change after startup.
- **No test-only behaviour in production code.** No hooks, flags, environment checks or
  parameters that exist only for tests (for example, to skip real waits or to fix the
  clock), so nothing in production can trigger them by accident. Tests change behaviour
  from the outside: patch the function or module attribute with `unittest.mock`. When the
  code under test runs in a child process, patch it in a test-only entry point under
  `tests/` (see `tests/crashing_listener.py`).
- **Use established libraries for standard formats and protocols** (python-hl7 for
  parsing) rather than writing our own. Write our own only where a library's behaviour
  would break a rule above, and say why in a comment.
- **Say exactly what happened.** Log lines, ACK texts and error messages must be accurate:
  "message not queued" when the file was saved but the publish failed, not "not stored".
- **Survive transient outages.** When a dependency such as Redis is down, wait and retry
  with a growing delay instead of crashing and relying on Docker to restart the process,
  and keep responding promptly to a stop request while waiting.
- **Comments explain why, briefly.** Don't restate the code, and don't copy rules from
  `AGENTS.md` or `DESIGN.md` into docstrings, where they drift; point to them instead.

## Tests

- **Anything checked by hand becomes a test:** killing Redis, stopping a container,
  crashing at a particular step.
- **Extend an existing test before writing a parallel one.** If the setup is the same, add
  a parametrized case (as the restart test does for its five ways of going down).
- **Use real processes and real Redis where the behaviour depends on them** (crashes,
  signals, outages). Use fakes in unit tests to pin down exact ordering and failures.
- **Make timing-dependent tests reliable.** Trigger events on progress (after N ACKs),
  not on sleeps; assert what must always hold, not exact counts.
- **A new test must be able to fail.** Break the behaviour it covers once and check that
  it fails.
- **Keep tests readable:** shared helpers and fixtures instead of repeated setup, and
  names that say what is checked.

## How to work

- Build one milestone from the `DESIGN.md` build plan at a time, on its own branch, and
  open a PR for each. Small, focused commits.
- **Keep PRs small.** A milestone can be several PRs. Put follow-ups in their own PR, and
  before adding to an open PR, say roughly how many lines the change adds.
- Each milestone needs tests, including the failure modes it touches (see the
  "Failure modes" table in `DESIGN.md`).
- Prefer the simplest thing that is correct. Add a dependency only when it clearly earns
  its place, and say why in the PR. When adding or upgrading a dependency or base image,
  check it for known CVEs, and pin downloads by hash where possible.
- If something in `DESIGN.md` turns out to be wrong or impractical, stop and explain the
  problem and options before changing direction.
- Keep the `README.md` current: what runs, how to start it, how to run tests.
- **Review before a PR is ready.** Have an independent adversarial review done (for example
  by a subagent):
  - Post its findings as a PR comment before fixing anything.
  - Confirm each blocker yourself, then fix it with a test that fails without the fix.
  - List the non-blockers in the comment for later; don't fix them unless asked.
- Keep the PR description up to date as the PR changes.
- **Commits:** author and committer are the repository owner, using their private GitHub
  email (the `users.noreply.github.com` address), never a personal one. Add a
  `Co-authored-by:` trailer naming the agent that actually did the work.

## After each milestone

In the PR description and in your final message, include:

1. What was built and how to run it.
2. The key decisions made and the alternatives considered.
3. Two or three key lessons from this milestone, each tied to the concrete failure it
   prevents (for example, why the listener stores a message before ACKing it).
