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

- **Simulated Hospital** (Google, open source, Docker) generates realistic ADT and result
  messages and sends them over MLLP:
  `-output mllp -mllp_destination <host>:<port>`.
  Inside Docker, `127.0.0.1` is the container, so point it at the host's address.
- Alternative: a small Python producer that fills HL7 templates with fake data. Gives full
  control over edge cases.
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
  1. Read the frame (`0x0B` … `0x1C 0x0D`).
  2. Write the exact bytes plus metadata to the raw archive.
  3. Publish a "new message" event to the queue.
  4. Send ACK `AA` only after steps 2–3 succeed. If storage fails, send `AE` or drop the
     connection so the sender retries.
- **No parsing before the ACK.** A message that can't be parsed must not block the feed;
  it gets quarantined downstream.
- Read only the minimum header fields needed for the file name and metadata (sending
  facility, message type, control ID), and tolerate even those failing.

### 4. Raw archive

- Local folder now; GCS or S3 later behind a `RawStore` interface (`save_raw(bytes, meta)`).
- Layout:
  `raw/hospital=<id>/feed=<adt|lab|...>/date=<YYYY-MM-DD>/<received_ts>_<sha256[:12]>.hl7`
  plus a `.json` file next to it holding the metadata (received_at, peer address, sha256,
  control ID, message type, listener version).
- Never overwrite: unique names and create-only writes. A resent message becomes a new file.
- Store bytes, not decoded text: segment separators are `\r`, and the character encoding
  varies (MSH-18).

### 5. Queue

- Redis Streams locally (consumer groups, `XACK`, pending entries, `XAUTOCLAIM` for
  redelivery). Pub/Sub later behind a `Publisher` interface.
- The event carries a pointer to the raw file, not the message itself.
- At-least-once and not ordered: consumers must handle duplicates and must not depend on
  arrival order.
- Messages that keep failing go to a dead-letter stream with the error attached.

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
| Malformed message | ACKed (it's stored), then quarantined at parse |
| Feed goes silent | Freshness alert per feed, aware of time of day |
| Burst after downtime | Queue absorbs it; listener only stores and ACKs |
| Listener crashes mid-message | No ACK sent, so the sender retries; duplicates handled downstream |
| Consumer falls behind | Lag alert; pending messages redelivered |

## Local stack

| Part | Tool |
|---|---|
| Producer | Simulated Hospital (Docker) or Python producer |
| Listener | Python asyncio + python-hl7 |
| Raw archive | Local folder (GCS later) |
| Queue | Redis Streams (Pub/Sub later) |
| Warehouse | DuckDB |
| Transformations | dbt (duckdb adapter) |
| Scheduling | Makefile / cron first; Dagster optional |
| API | FastAPI |
| Everything | Docker Compose |

## Build plan

1. **Getting data in:** docker-compose with Simulated Hospital, the listener, the raw
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
