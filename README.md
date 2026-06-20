# h-obs

Home Observability (h-obs) — a simple, durable approach for ingesting low-volume household telemetry from cloud HTTP APIs every minute, storing it in a directly queryable format, and keeping long-term portability for 10–20 years.

## Goal

Build a **fully managed, low-ops pipeline on Google Cloud** that:

- polls external HTTP APIs every minute,
- stores data in a format that remains useful over decades,
- keeps data directly queryable/aggregatable,
- avoids lock-in through open export paths.

## Recommended Architecture (Google Cloud end-to-end)

1. **Cloud Scheduler** triggers every minute.
2. **Cloud Run** service/job calls the external API.
3. Ingestion writes one record per call to **BigQuery** (primary store).
4. Optional safety archive writes raw payloads to **GCS** as `jsonl.gz`.
5. Periodic export from BigQuery to **Parquet in GCS** for long-term portability.
6. Visualization and alerting can be done with:
   - native GCP tools (Cloud Monitoring / Looker Studio), or
   - Grafana (if preferred for cross-source dashboards).

```text
Cloud Scheduler (*/1 * * * *)
        |
        v
  Cloud Run poller  ----->  GCS raw archive (optional, JSONL.gz)
        |
        v
 BigQuery raw_ingestion table (queryable)
        |
        v
 Scheduled export to GCS Parquet (portable cold copy)
```

## Why this approach

For small household data volumes, BigQuery keeps things simple and directly queryable while staying fully managed.

- **Low operational overhead**: no DB servers to run.
- **SQL-first analysis**: immediate aggregation and dashboards.
- **Long-term durability**: archive/export path to open formats.
- **Future migration safety**: Parquet exports prevent hard lock-in.

## Data format strategy

### Primary query store: BigQuery

Use an append-only table with partitioning and clustering.

Suggested fields:

- `fetched_at TIMESTAMP` — ingestion timestamp (UTC)
- `source STRING` — provider name
- `endpoint STRING` — API endpoint identifier
- `status_code INT64` — HTTP status code
- `payload JSON` — full raw API response
- `payload_hash STRING` — SHA-256 of raw payload
- `request_id STRING` — ingestion request/correlation id

Partition by `DATE(fetched_at)` and cluster by `source, endpoint`.

### Raw archival format: JSONL (optional but recommended)

Store one response envelope per line in `jsonl.gz`:

- preserves nested API structure,
- tolerant to schema changes over time,
- ideal for replay/reprocessing,
- line-oriented and compression-friendly.

### Portability format: Parquet

Export curated/raw tables periodically to Parquet in GCS for long-term, engine-agnostic access.

## JSONL vs CSV (decision)

JSONL is preferred for raw API ingestion because API responses are often nested and evolve over time.

- JSONL is lossless for nested/variable payloads.
- CSV requires flattening and can lose structure/context.
- CSV is still fine for simple flat exports, but not ideal as canonical raw store.

## Long-term retention principles (10–20 years)

1. Keep immutable append-only ingestion records.
2. Preserve full raw payload (`payload JSON`) for replay.
3. Maintain periodic Parquet exports in GCS.
4. Version schemas and document units/timezones.
5. Run annual restore/migration drills from Parquet to a fresh target.

## Minimal implementation plan

1. Create BigQuery dataset and partitioned table.
2. Implement Cloud Run poller with retry/backoff and idempotent writes.
3. Configure Cloud Scheduler to run every minute.
4. Add optional GCS JSONL archive writes.
5. Add scheduled BigQuery → GCS Parquet export.
6. Add dashboards and alerts (GCP native or Grafana).

## Example storage paths

Raw archive in GCS:

```text
gs://<bucket>/source=<provider>/year=YYYY/month=MM/day=DD/hour=HH/minute=mm/<timestamp>_<request-id>.jsonl.gz
```

Parquet export:

```text
gs://<bucket>/parquet/source=<provider>/year=YYYY/month=MM/day=DD/part-*.parquet
```

## Notes on Grafana Cloud retention

Grafana Cloud is excellent for visualization/alerting, but typical plan retention windows may not match multi-decade history requirements. In this architecture, BigQuery/GCS are the long-term system of record.

---

This repository documents the architecture and implementation for this approach.
