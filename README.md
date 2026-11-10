# WasteStream Lakehouse

A contract-driven data pipeline for smart-bin telemetry. It turns noisy sensor events into a medallion-style lakehouse with immutable bronze records, validated silver events, quarantined failures, and gold operational metrics.

## The data problem

IoT feeds arrive late, contain duplicate events, and sometimes report impossible values. A dashboard should never silently treat those records as truth. This project makes every quality decision auditable.

```text
CSV events
   |
   +--> bronze/events.jsonl          raw, append-friendly records
   |
   +--> silver/events.jsonl          typed, valid, deduplicated events
   +--> quarantine/rejected.jsonl    rejected rows + explicit reasons
   |
   +--> gold/daily_metrics.csv       fill, temperature, and alert aggregates
```

## Run locally

The reference pipeline uses only the Python standard library:

```bash
PYTHONPATH=src python -m waste_lakehouse.pipeline \
  --input data/sample_sensor_events.csv \
  --output build

PYTHONPATH=src python -m unittest discover -s tests -v
```

The repository also includes `databricks/medallion_job.py`, a PySpark implementation of the same contract for a Databricks workflow.

## Quality rules

- `event_id` and `sensor_id` are required
- timestamps must use ISO 8601
- fill percentage must be between 0 and 100
- temperature must be between -30 and 100 C
- duplicate `event_id` values are quarantined
- rejected rows retain their raw values and validation reasons

## Recruiter walkthrough

1. Start with `contracts/sensor_event.schema.json` to see the source contract.
2. Read `pipeline.py` to follow validation, deduplication, and aggregation.
3. Run the tests to see invalid ranges and duplicate events handled explicitly.
4. Compare the local implementation with the included PySpark job.

## Production roadmap

- Auto Loader ingestion with checkpoints and schema evolution
- Delta Live Tables expectations and Unity Catalog lineage
- Watermarks for late events and idempotent merge keys
- Great Expectations or Deequ quality history
- Power BI operational dashboard over gold tables

