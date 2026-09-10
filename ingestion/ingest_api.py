"""
ingest_api.py

Pulls events from the mock order-status API (mock_api_source.py) and loads
them into BigQuery's raw.api_source_events table.

Normal runs use a saved checkpoint (max sequence successfully loaded so
far) and request only events after it via since_sequence — avoiding a
full re-scan of the entire dataset on every run. Filtering is based on
sequence (true creation order), not status_changed_at, because
status_changed_at can be backdated by late-arriving data and would
silently and permanently exclude genuinely new events.

Backfill runs explicitly override the checkpoint via command-line flags,
to deliberately reprocess a specific historical sequence range without
disturbing the normal checkpoint's progress.

Schema drift detection: compares each event's actual fields against an
explicit expected schema. New fields are logged to raw.api_schema_change_log,
and the target table's schema is explicitly widened (ALTER TABLE) before
insert, since streaming inserts require the column to pre-exist.

Idempotency: event_id-based dedup log acts as a safety net on top of the
checkpoint, in case of retried or overlapping runs.

Pagination: walks the API via cursor until next_cursor is null, within
whatever since_sequence/until_sequence window applies for this run.
"""

import argparse
import os
import time
from datetime import datetime, timezone

import requests
from google.cloud import bigquery

PROJECT_ID = "credible-skill-453411-c1"
DATASET = "raw"
TARGET_TABLE = "api_source_events"
LOG_TABLE = "api_ingestion_log"
SCHEMA_LOG_TABLE = "api_schema_change_log"
CHECKPOINT_TABLE = "api_ingestion_checkpoint"
SOURCE_NAME = "api_source"

API_BASE_URL = os.environ.get("MOCK_API_URL", "http://localhost:8000")
PAGE_LIMIT = 20

EXPECTED_FIELDS = {
    "event_id",
    "sequence",
    "order_id",
    "status",
    "status_changed_at",
    "updated_by",
}

client = bigquery.Client(project=PROJECT_ID)


def get_checkpoint():
    """Returns the saved checkpoint (max sequence loaded so far), or None."""
    query = f"""
        SELECT last_sequence
        FROM `{PROJECT_ID}.{DATASET}.{CHECKPOINT_TABLE}`
        WHERE source_name = @source_name
        ORDER BY updated_at DESC
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source_name", "STRING", SOURCE_NAME)
        ]
    )
    results = list(client.query(query, job_config=job_config).result())
    if not results:
        return None
    return results[0]["last_sequence"]


def save_checkpoint(new_checkpoint: int):
    """Records the new checkpoint after a successful normal run."""
    table_ref = f"{PROJECT_ID}.{DATASET}.{CHECKPOINT_TABLE}"
    row = {
        "source_name": SOURCE_NAME,
        "last_sequence": new_checkpoint,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        raise RuntimeError(f"Failed to save checkpoint: {errors}")
    print(f"Checkpoint updated to sequence {new_checkpoint}")


def get_already_loaded_event_ids() -> set:
    """Safety-net dedup check, in case of overlapping/retried runs."""
    query = f"""
        SELECT event_id
        FROM `{PROJECT_ID}.{DATASET}.{LOG_TABLE}`
        WHERE source_name = @source_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source_name", "STRING", SOURCE_NAME)
        ]
    )
    results = client.query(query, job_config=job_config).result()
    return {row["event_id"] for row in results}


def fetch_all_events(since_sequence=None, until_sequence=None) -> list:
    """
    Walks the API via cursor pagination until next_cursor is null.
    since_sequence scopes which events the API returns at all.
    until_sequence (client-side only) optionally caps a backfill window.
    """
    all_events = []
    cursor = None

    while True:
        params = {"limit": PAGE_LIMIT}
        if cursor:
            params["cursor"] = cursor
        if since_sequence is not None:
            params["since_sequence"] = since_sequence

        response = requests.get(f"{API_BASE_URL}/events", params=params)
        response.raise_for_status()
        payload = response.json()

        page = payload["data"]
        if until_sequence is not None:
            page = [e for e in page if e["sequence"] <= until_sequence]

        all_events.extend(page)
        cursor = payload["next_cursor"]

        if cursor is None:
            break

    return all_events


def detect_new_fields(event: dict) -> list:
    event_fields = set(event.keys())
    return sorted(event_fields - EXPECTED_FIELDS)


def log_schema_change(event_id: str, new_fields: list):
    table_ref = f"{PROJECT_ID}.{DATASET}.{SCHEMA_LOG_TABLE}"
    row = {
        "event_id": event_id,
        "detected_new_columns": ", ".join(new_fields),
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }
    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        raise RuntimeError(f"Failed to log schema change for {event_id}: {errors}")
    print(f"  SCHEMA DRIFT DETECTED in {event_id}: new field(s) -> {new_fields}")


def ensure_column_exists(field_name: str):
    table_ref = f"{PROJECT_ID}.{DATASET}.{TARGET_TABLE}"
    table = client.get_table(table_ref)

    existing_field_names = {f.name for f in table.schema}
    if field_name in existing_field_names:
        return

    new_schema = table.schema[:]
    new_schema.append(bigquery.SchemaField(field_name, "STRING", mode="NULLABLE"))
    table.schema = new_schema
    client.update_table(table, ["schema"])
    print(f"  Table schema updated: added column '{field_name}'")

    for attempt in range(10):
        time.sleep(2)
        refreshed_table = client.get_table(table_ref)
        refreshed_field_names = {f.name for f in refreshed_table.schema}
        if field_name in refreshed_field_names:
            print(f"  Confirmed column '{field_name}' is live (after {(attempt + 1) * 2}s).")
            return

    raise RuntimeError(f"Column '{field_name}' did not become visible after waiting.")


def insert_event_to_bigquery(event: dict):
    table_ref = f"{PROJECT_ID}.{DATASET}.{TARGET_TABLE}"
    row = dict(event)
    row["ingested_at"] = datetime.now(timezone.utc).isoformat()

    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        raise RuntimeError(f"Failed to insert event {event['event_id']}: {errors}")


def log_ingested_event(event_id: str):
    table_ref = f"{PROJECT_ID}.{DATASET}.{LOG_TABLE}"
    row = {
        "event_id": event_id,
        "source_name": SOURCE_NAME,
        "loaded_at": datetime.now(timezone.utc).isoformat(),
    }
    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        raise RuntimeError(f"Failed to log event {event_id}: {errors}")


def run(backfill_since_sequence=None, backfill_until_sequence=None):
    is_backfill = backfill_since_sequence is not None

    if is_backfill:
        print(f"BACKFILL MODE: since_sequence={backfill_since_sequence}, "
              f"until_sequence={backfill_until_sequence}")
        since_sequence = backfill_since_sequence
        until_sequence = backfill_until_sequence
    else:
        since_sequence = get_checkpoint()
        until_sequence = None
        print(f"Normal run. Checkpoint: {since_sequence if since_sequence is not None else '(none — first run)'}")

    already_loaded = get_already_loaded_event_ids()

    all_events = fetch_all_events(since_sequence=since_sequence, until_sequence=until_sequence)
    print(f"Fetched {len(all_events)} event(s) for this window.")

    new_count = 0
    max_sequence = since_sequence
    for event in all_events:
        event_id = event["event_id"]

        if event_id in already_loaded:
            print(f"SKIP  {event_id} (already loaded)")
            continue

        new_fields = detect_new_fields(event)
        if new_fields:
            log_schema_change(event_id, new_fields)
            for field in new_fields:
                ensure_column_exists(field)

        insert_event_to_bigquery(event)
        log_ingested_event(event_id)
        print(f"LOADED {event_id} (sequence {event['sequence']})")
        new_count += 1

        if max_sequence is None or event["sequence"] > max_sequence:
            max_sequence = event["sequence"]

    print(f"\nDone. {new_count} new event(s) loaded, "
          f"{len(all_events) - new_count} skipped (already loaded).")

    if not is_backfill and max_sequence is not None and max_sequence != since_sequence:
        save_checkpoint(max_sequence)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill-since-sequence", type=int, help="start backfill after this sequence")
    parser.add_argument("--backfill-until-sequence", type=int, help="end backfill at this sequence (inclusive)")
    args = parser.parse_args()

    run(
        backfill_since_sequence=args.backfill_since_sequence,
        backfill_until_sequence=args.backfill_until_sequence,
    )