# Clinical Data Pipeline — Rough Design

A local pipeline for synthetic hospital data. It starts with HL7 v2 messages from a
simulated hospital and is built so FHIR can be added as a second source later. Data gets
stored, parsed, mapped and modeled, and comes out as feature tables, reporting tables and
a small read API. The point is to practice the hard parts of clinical data (duplicates,
corrections, late data, replay), not to build at scale.

Synthetic data only. No real patient data ever goes in this repo.

## Goals

- Receive HL7 v2 over MLLP the way a vendor receives a hospital feed.
- Never lose a message: store first, acknowledge second.
- Keep the exact raw bytes forever so anything can be replayed or traced.
- Model clinical data correctly when messages are duplicated, out of order, corrected,
  cancelled or merged.
- Produce point-in-time-correct features, analytics tables and a small read API.
- Catch bad data with checks at each handoff, and recover with replays from raw.

## Non-goals (for now)

- Real hospital connectivity (VPN, interface-engine configuration).
- Production-grade security or multi-tenant isolation (noted, not built).
- Scale. One simulated hospital is enough; the design should allow adding more.

## Architecture

```
                                      GETTING DATA IN
 ┌──────────────────┐   ┌──────────────┐   ┌──────────────┐   ┌───────────────┐   ┌────────────┐
 │ Simulated        │──▶│ Messiness    │──▶│ MLLP         │──▶│ Raw archive   │──▶│ Queue      │
 │ Hospital (HL7v2) │   │ injector     │   │ listener     │   │ (exact bytes) │   │ (events)   │
 └──────────────────┘   └──────────────┘   │ save → ACK   │   └───────────────┘   └─────┬──────┘
                                           └──────────────┘                             │
                                      MODELED DATA                                      ▼
                        ┌──────────────┐   ┌──────────────────┐   ┌──────────────────────────┐
                        │ Parser       │──▶│ Mapping and      │──▶│ Canonical model          │
                        │ + quarantine │   │ identity         │   │ (versioned, 2 timestamps)│
                        └──────────────┘   └──────────────────┘   └────────────┬─────────────┘
                                      OUTPUTS                                   │
                        ┌──────────────┐   ┌──────────────────┐   ┌────────────▼─────────────┐
                        │ ML features  │   │ Analytics marts  │   │ API read model + FastAPI │
                        │ point-in-time│   │                  │   │                          │
                        └──────────────┘   └──────────────────┘   └──────────────────────────┘

 Across every stage: quality checks · orchestration and replay · monitoring and lineage · security
```

## Components

### 1. Producer

- **Python producer (current, `producer/`)** fills HL7 templates (ADT A01/A08/A03,
  ORU^R01) with synthetic data and sends them over MLLP. It behaves like a hospital
  interface engine: one message at a time, wait for the ACK, and resend the same message
  on `AE`, timeout or a dropped connection until it gets `AA`. Full control over edge
  cases, and the integration tests use it to know exactly which messages were ACKed.
- **Simulated Hospital** (Google, open source) generates more realistic ADT and result
  messages over MLLP (`-output mllp -mllp_destination <host>:<port>`). Its published image
  (`eu.gcr.io/simhospital-images/simhospital`) currently refuses unauthenticated pulls,
  and the source only builds with Bazel, so it is deferred: revisit at milestone 3–4,
  when realistic variety starts to matter, as a second producer. Inside Docker,
  `127.0.0.1` is the container, so point it at the listener's service name.
- Later: Synthea CSVs as a stand-in for a historical reporting-database extract.

### 2. Messiness injector

Sits between the producer and the listener (an MLLP proxy) and randomly:

- resends a message (duplicate)
- holds a message back and sends it later (out of order)
- sends a corrected result (OBX-11 = `C`) hours after the final (`F`)
- merges two patients (ADT A40) or cancels an admission (A11)
- changes a test's units (mg/dL ↔ mmol/L) or starts using a new local lab code
- adds a custom Z-segment
- sends a malformed message
- goes silent for a while, then sends a burst

Every injected change is logged so checks can be scored against what was actually injected.

### 3. MLLP listener

- Async TCP server (python-hl7 `hl7.mllp`) on port 2575.
- For each message:
  1. Read the frame (`0x0B` … `0x1C 0x0D`). Whitespace before the `0x0B` (e.g. a newline
     some senders put between frames) is skipped; any other bytes there are kept in
     `data/rejected/` as evidence, and the frame itself is processed normally. A later
     `0x0B` inside the frame is kept as content, unless it starts a new `MSH`: then the
     sender abandoned the earlier frame, so that part is kept in `data/rejected/` as
     evidence with no reply, and only the new message is processed.
  2. Write the exact bytes plus metadata to the raw archive.
  3. Publish a "new message" event to the queue.
  4. Send ACK `AA` only after steps 2–3 succeed. If either fails, send `AE` so the
     sender resends (a raw file without an event is just a future duplicate).
- **No parsing before the ACK.** A message with bad contents must not block the feed:
  it is stored, ACKed and quarantined downstream.
- Read only the minimum header fields needed for the file name and metadata (sending
  facility, message type, control ID). A leading UTF-8 byte-order mark is skipped for
  reading only.
- **Unidentifiable messages are rejected.** If even those header fields can't be read (no
  MSH, control ID or message type), there is nothing to match an ACK to and nothing to
  queue. Keep the exact bytes as evidence in `data/rejected/` (same layout as the raw
  archive, never queued), then reply `AR` with the reason. If saving the evidence fails,
  reply `AE` so the sender retries. This matches Google's HL7v2 store, which rejects
  messages without a header but can store and flag content errors (`SOFT_FAIL`).

### 4. Raw archive

- Local folder now; GCS or S3 later behind a `RawStore` interface (`save_raw(bytes, meta)`).
- Layout:
  `raw/hospital=<id>/feed=<type>/date=<YYYY-MM-DD>/<received_ts>_<sha256[:12]>.hl7`
  plus a `.json` file next to it holding the metadata (received_at, peer address, sha256,
  control ID, message type, listener version).
  - `hospital` is the first component of MSH-4 (sending facility) and `feed` is the
    message code from MSH-9 (`adt`, `oru`, ...), lowercased and reduced to `[a-z0-9_-]`
    so header values can't escape the folder. Both are `unknown` if the header can't be
    read. `date` and `received_ts` are UTC, from when we received the message.
- Never overwrite: unique names and create-only writes. A resent message becomes a new file;
  the same bytes in the same microsecond get a `_1`, `_2` suffix.
- Write to a hidden temp file, fsync, then hard-link to the final name. The link fails if
  the name exists (so nothing is overwritten), and a final name only ever points at a
  complete file: a crash mid-write leaves a stray `.tmp-*` file, never a truncated message.
- Store bytes, not decoded text: segment separators are `\r`, and the character encoding
  varies (MSH-18).

### 5. Queue

- Redis Streams locally (consumer groups, `XACK`, pending entries, `XAUTOCLAIM` for
  redelivery). Pub/Sub later behind a `Publisher` interface.
- The event carries a pointer to the raw file, not the message itself: `raw_key`,
  `raw_message_id` (sha256), `source_system`, hospital, feed, message type, control ID,
  received_at.
- Redis runs with an append-only file fsynced on every write, so an event the listener
  published (and then ACKed) survives a Redis restart.
- At-least-once and not ordered: consumers must handle duplicates and must not depend on
  arrival order.
- **Dead-letter queue (`hl7.dlq`, behind the same `Publisher` interface; a Pub/Sub
  dead-letter topic later).** One entry per bad input, never per log line: `stage`
  (`listener` now; later stages add entries for messages they keep failing on), `kind`
  (`unidentifiable`, `noise_before_frame`, `no_start_byte`, `cut_off_by_disconnect`,
  `too_large`, `abandoned_frame`), `reason`, the `reply` sent (`AR` or `none`), sha256,
  size, and `evidence_key` pointing at the exact bytes in `data/rejected/`. If that file
  couldn't be saved, the entry carries the bytes as `payload_b64`. `make dlq` shows the
  latest entries. Bad input is counted, alerted on and replayed from here.

### 6. Parser

- Reads the raw file and writes staging rows:
  `stg_messages` (one per message), `stg_patients` (PID), `stg_visits` (PV1),
  `stg_observations` (one per OBX), `stg_orders` (OBR).
- Every row keeps `raw_message_id` (sha256) for lineage.
- Failures go to `quarantine` with the error and raw pointer; the pipeline keeps going.
- Removes duplicates by (hospital, message control ID), with the raw hash as backup.

### 7. Mapping and identity

- Per-hospital config file (YAML): local code → LOINC, expected units and conversions,
  how to read custom Z-segments, the hospital's time zone.
- Unmapped codes are kept and flagged, never silently dropped.
- Identity: (hospital, MRN) → internal `patient_key`. A40 merges add a row to a merge
  table; history is not rewritten.

### 8. Canonical model

- Tables: `patients`, `encounters`, `observations`, `orders`, `patient_merges`.
- Append-only versions. Each version has:
  - `event_time`: when it happened clinically (e.g. collection or result time)
  - `received_at`: when we learned it
  - `status`: final, corrected, cancelled
- A correction or cancellation is a new version, so we can always answer "what did we know
  at time T?"

### 9. Outputs

- **ML features:** built "as of" a prediction time, using only versions with
  `received_at <= T`. Example: latest potassium as known at T. Datasets are versioned so
  training runs can be reproduced.
- **Analytics marts:** aggregated tables (volumes, turnaround times, abnormal-result rates).
- **API read model:** precomputed latest values per patient, served by FastAPI
  (`GET /patients/{id}/labs/latest`), with a freshness timestamp on every response.

### 10. Across every stage

- **Quality checks:** schema, volume by hour vs. normal, freshness, duplicate rate,
  unmapped-code rate, units per lab code, value ranges, orphan results. Each check is set
  to block, quarantine or alert.
- **Orchestration and replay:** jobs are split by time partition and safe to re-run. A
  backfill is "re-run from the raw archive for this hospital and date range", with
  outputs written to a new version and then swapped in.
- **Monitoring and lineage:** queue lag, time since last message per feed, parse failure
  rate. Any API value can be traced to its canonical row, staging row and raw file.
- **Security (noted, not built):** encryption, access control, audit logs, per-hospital
  separation.

## Key decisions and trade-offs

| Decision | Choice for now | Alternative | Why |
|---|---|---|---|
| When to ACK | After raw write + queue publish | ACK on receipt | Never acknowledge something we could lose |
| Raw format | Exact bytes + metadata file | Parsed JSON only | Parser bugs can be fixed and replayed |
| Raw layout | One file per message | Batched files | Simple and immediate; combine into Parquet later for analytics |
| Create-only writes | Temp file + fsync + hard link | `open(..., "x")` directly | Never overwrites, and a crash can't leave a truncated file under a real name |
| Queue durability | Redis AOF, `appendfsync always` | Default snapshots / `everysec` | An ACKed event must survive a Redis restart; the speed cost doesn't matter at this scale |
| Producer | Python producer now; Simulated Hospital later | Build Simulated Hospital from source now | Its image won't pull and the source needs Bazel; milestone 1 only needs correct MLLP + resend behavior |
| Frame reading | Own `read_frame` on python-hl7's streams, starting at the first `0x0B` | python-hl7 `readblock()` | `readblock()` drops a whole message if any byte precedes `0x0B` |
| Record of bad input | Dead-letter stream (`hl7.dlq`) entry pointing at an evidence file in `data/rejected/` | Log lines only; or evidence files only | Log lines get lost and can't be acted on. Files alone can't be counted, alerted on or worked through. A DLQ entry is durable, countable and replayable, and maps to Pub/Sub dead-letter topics |
| Malformed messages at intake | `AR` only when the header can't be read (bytes kept as evidence); everything else stored, `AA`, quarantined at parse | Reject every malformed message; or accept everything, even with no header | Rejecting content errors stalls the sender's queue and loses evidence, and our own parser bugs would block their feed. But an `AA` with no control ID is useless to the sender |
| Triggering processing | Listener publishes to queue | Storage-event notification (GCS → Pub/Sub) | Both are fine; interface keeps it swappable |
| Parsing | Own parser (python-hl7) | GCP HL7v2 store + HL7→FHIR | Full control over mapping, history and replay |
| Warehouse | DuckDB + dbt | BigQuery | Free and local; dbt models carry over |
| Cloud | Local-first behind interfaces | Cloud from day one | Fast to iterate; swap GCS / Pub/Sub in at the end |

## Failure modes and how they're handled

| What goes wrong | Handling |
|---|---|
| Duplicate message | Deduplicate on (hospital, control ID) with hash as backup |
| Out-of-order delivery | Order by `event_time` and versions, never by arrival |
| Corrected or cancelled result | New version; features use what was known at T |
| Patient merge (A40) | Merge table maps old key to surviving key |
| Unit change | Unit check per lab code alerts; conversion from site config |
| New local code | Kept and flagged; unmapped-code rate alert |
| Malformed message (readable header) | ACKed (it's stored), then quarantined at parse |
| Unidentifiable message (no MSH, control ID or message type) | Bytes kept in `data/rejected/`, never queued; `AR` with the reason |
| Saving rejected evidence, or its DLQ entry, fails | `AE`, so the sender retries (the `AR` waits for both) |
| Feed goes silent | Freshness alert per feed, aware of time of day |
| Burst after downtime | Queue absorbs it; listener only stores and ACKs |
| Listener crashes mid-message | No ACK sent, so the sender retries; duplicates handled downstream |
| Listener crashes mid-write | Temp file + hard link: no truncated file under a final name; stray `.tmp-*` files are harmless |
| Redis down or publish fails | Listener sends `AE`; sender resends; the orphan raw file is a harmless duplicate |
| Redis restarts | AOF with fsync on every write keeps already-ACKed events |
| Stray bytes between frames | Whitespace is skipped; anything else is kept in `data/rejected/` with the reason. The message after it is processed normally |
| Block with an end marker but no `0x0B` | Kept whole in `data/rejected/` with the reason; no reply (it isn't a frame). The stream stays in sync for the next frame |
| Connection closed mid-frame | The incomplete bytes are kept in `data/rejected/`; never ACKed, so the sender resends |
| Frame over the size limit (`MAX_MESSAGE_BYTES`) | The bytes received so far are kept in `data/rejected/`, then the connection is closed (the next frame boundary can't be found reliably) |
| Saving evidence to `data/rejected/` fails (input with no reply) | The DLQ entry carries the bytes themselves (`payload_b64`); never holds up the messages around it |
| DLQ unreachable (input with no reply) | Last resort: a log line with the bytes' size, sha256 and evidence file |
| Abandoned partial frame (a new `0x0B` + `MSH` before the end marker) | The fragment is kept in `data/rejected/` with no reply; the new message is stored and ACKed on its own |
| Stray `0x0B` inside a message | Kept as content (it doesn't start a new `MSH`); the message is stored whole |
| HL7 batch in one frame (`FHS`/`BHS` instead of `MSH`) | Out of scope for now: gets `AR`, bytes kept in `data/rejected/`. Real-time MLLP feeds send single messages; batches mostly arrive as files |
| Multibyte charset header (Big5, GB 18030, ISO-2022-JP; MSH-18) with non-ASCII text in MSH-3–8 | Known limitation: a character's second byte can equal `|`, shifting the fields, so the message can be wrongly rejected (`AR`). Fix later by decoding the header per MSH-18 before splitting; test it with the messiness injector (milestone 2) |
| Consumer falls behind | Lag alert; pending messages redelivered |

## Local stack

| Part | Tool |
|---|---|
| Producer | Python producer (Simulated Hospital later) |
| Listener | Python asyncio + python-hl7 |
| Raw archive | Local folder (GCS later) |
| Queue | Redis Streams (Pub/Sub later) |
| Warehouse | DuckDB |
| Transformations | dbt (duckdb adapter) |
| Scheduling | Makefile / cron first; Dagster optional |
| API | FastAPI |
| Everything | Docker Compose |

## Build plan

1. **Getting data in:** docker-compose with the Python producer, the listener, the raw
   archive and Redis. Done when messages are stored, ACKed and queued, and killing the
   listener mid-stream loses nothing.
2. **Messiness injector:** proxy with configurable chaos and a log of what it injected.
3. **Parser + quarantine:** staging tables in DuckDB with lineage.
4. **Mapping and canonical model:** site config, identity, versioned observations (dbt).
5. **Checks:** catch everything the injector does; compare against its log.
6. **Replay drill:** ship a unit-conversion bug, fix it, re-run from raw, compare outputs.
7. **Outputs:** point-in-time features, a mart, the FastAPI endpoint.
8. **Optional cloud swap:** GCS + Pub/Sub behind the existing interfaces.
9. **FHIR source:** see below.

## Extending to FHIR

FHIR is a second source with its own adapter, parser and mapping. Everything from
the canonical model onward (features, marts, API, checks) stays the same.

| Stage | HL7 v2 (now) | FHIR (later) |
|---|---|---|
| How data arrives | Hospital pushes over MLLP | We pull: regular API queries, or Bulk FHIR `$export` |
| Ingestion component | MLLP listener | Scheduled poller that keeps a checkpoint (`_lastUpdated` / `_since`) |
| Raw archive | `.hl7` bytes | JSON / NDJSON as received, under `source=fhir/resource=<type>/...` |
| What a record means | An event ("this happened") | A snapshot ("this is how it looks now") |
| Detecting changes | Each message is a change | Compare with the previous version (`meta.versionId`, `meta.lastUpdated`); deletions need explicit handling |
| Parser | Segments and fields | Resources (Patient, Encounter, Observation, MedicationRequest) |
| Codes | Often local codes | Often LOINC/SNOMED already, but still mapped and checked |
| Identity | (hospital, MRN) from PID-3 | `Patient.identifier` (system + value) → same `patient_key` |

Local test setup: run a HAPI FHIR server in Docker and load it with Synthea's FHIR or
Bulk FHIR output. The poller then pulls from it like a real EHR.

Design rules that keep this easy:

- The raw store, queue and canonical model don't depend on the source format.
- Every canonical row records its `source_system` and `raw_message_id` for lineage.
- If the same fact arrives from both HL7 and FHIR, it is matched and deduplicated in the
  canonical model, with a defined rule for which source wins.

## Open questions

- How to recognize the end of an encounter for features that need it (discharge vs. A11
  cancel vs. missing A03)?
- How strict should the parser be on messages that bend the standard?
- How long should quarantined messages wait before someone is alerted?
- When a mapping changes, which outputs get rebuilt automatically?
