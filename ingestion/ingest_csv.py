"""
Reads locally-generated CSV files (from generate_csv_source.py) and loads
them into BigQuery's raw.csv_source_orders table.

Idempotent at the file level: checks raw.csv_ingestion_log before loading any
file, so re-running this script never reprocesses a file already loaded.
(Exact row-level dedup — e.g. the intentional duplicate row the generator
sometimes injects — is intentionally NOT handled here. Raw/bronze layer
should faithfully mirror the source file, duplicates included. Dedup is
handled downstream in dbt staging via incremental + unique_key merge.)

Schema drift detection: compares each file's header against the expected
schema. Any unexpected new column is logged explicitly to
raw.csv_schema_change_log rather than being silently absorbed.

Late-arriving data: no special handling here by design — raw faithfully
loads whatever a file contains, regardless of what event_timestamp says.
Late data is correctly captured downstream because staging incrementality
is based on arrival time (ingested_at), not event time.

Backfill support: every row is tagged with source_file (the exact filename
it came from). This makes backfill unambiguous — deleting/reloading one
file's rows never risks touching a late row that physically lives in a
different file but happens to share a timestamp range.

Note: if raw.csv_source_orders doesn't exist yet, this script creates it
automatically using EXPECTED_SCHEMA as the blueprint
"""

import csv
import io
import os
from datetime import datetime, timezone
from google.cloud import bigquery

from dotenv import load_dotenv
load_dotenv()

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = "raw"
TARGET_TABLE = "csv_source_orders"
LOG_TABLE = "csv_ingestion_log"
SCHEMA_LOG_TABLE = "cav_schema_change_log"
SOURCE_NAME = "csv_source"
INPUT_DIR = "data_generation/output/csv_source"

# Explicit expected schema — this is the source of truth, not autodetect.
EXPECTED_SCHEMA = [
    bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("product_category", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("amount", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED")

    # "payment_method" is deliberately NOT here — this is the column
    # the generator starts adding after file index 10, which is what
    # makes it "unexpected" when detect_new_columns() checks for it.
]
EXPECTED_COLUMN_NAMES = {field.name for field in EXPECTED_SCHEMA if field.name != "source_file"}

client = bigquery.Client(project=PROJECT_ID)


def get_already_loaded_files() -> set:
    """Return the set of filenames already recorded in the ingestion log."""
    query = f"""
        SELECT filename
        FROM `{PROJECT_ID}.{DATASET}.{LOG_TABLE}`
        WHERE source_name = @source_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source_name", "STRING", SOURCE_NAME)
        ]
    )
    results = client.query(query, job_config=job_config).result()
    return {row["filename"] for row in results}


def read_csv_rows(filepath: str) -> tuple:
    """
    Reads a CSV file fully into memory.
    Returns (header, rows) where rows is a list of dicts.
    """
    with open(filepath, "r", newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        rows = list(reader)
    return header, rows


def detect_new_columns(header: list) -> list:
    """Returns any columns in the file's header that aren't in EXPECTED_SCHEMA."""
    file_columns = set(header)
    new_columns = file_columns - EXPECTED_COLUMN_NAMES
    return sorted(new_columns)


def log_schema_change(filename: str, new_columns: list):
    """Records a detected schema change so it's visible and reviewable."""
    table_ref = f"{PROJECT_ID}.{DATASET}.{SCHEMA_LOG_TABLE}"
    row = {
        "filename": filename,
        "detected_new_columns": ", ".join(new_columns),
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }
    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        raise RuntimeError(f"Failed to log schema change for {filename}: {errors}")
    print(f"  SCHEMA DRIFT DETECTED in {filename}: new column(s) -> {new_columns}")

def build_schema_for_load(new_columns: list) -> list:
    """
    Returns the schema to use for this specific load job.
    If new_columns is empty, returns EXPECTED_SCHEMA unchanged.
    If new_columns has entries, appends them as NULLABLE STRING fields.
    """
    extended_schema = list(EXPECTED_SCHEMA)  
    for col in new_columns:
        extended_schema.append(
            bigquery.SchemaField(col, "STRING", mode="NULLABLE")
        )
    extended_schema.append(bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"))    
    return extended_schema   

def rows_to_csv_buffer(rows: list, fieldnames: list) -> io.StringIO:
    """
    Writes a list of row dicts to an in-memory CSV buffer (no file on disk),
    ready to be loaded into BigQuery.
    """
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    buffer.seek(0)
    return buffer


def load_file_to_bigquery(filename: str, filepath: str, new_columns: list) -> int:
    """
    Loads one CSV file into the target table, tagging every row with
    source_file. Returns number of rows loaded.
    """
    table_ref = f"{PROJECT_ID}.{DATASET}.{TARGET_TABLE}"

    header, rows = read_csv_rows(filepath)

    # Inject source_file into every row before loading.
    for row in rows:
        row["source_file"] = filename

    base_columns = [f.name for f in EXPECTED_SCHEMA]
    load_fieldnames = base_columns + new_columns + ["source_file"]
    buffer = rows_to_csv_buffer(rows, load_fieldnames)

    load_schema = build_schema_for_load(new_columns)
    schema_update_options = (
        [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        if new_columns
        else []
    )

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=load_schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=schema_update_options,
    )

    load_job = client.load_table_from_file(
        buffer, table_ref, job_config=job_config
    )

    try:
        load_job.result()
    except Exception as e:
        print(f"  LOAD FAILED for {filepath}")
        if load_job.errors:
            for err in load_job.errors:
                print(f"    - {err}")
        raise RuntimeError(f"Load job failed for {filepath}: {e}")

    return load_job.output_rows


def log_ingested_file(filename: str, row_count: int):
    """Records a successfully loaded file in the ingestion log."""
    table_ref = f"{PROJECT_ID}.{DATASET}.{LOG_TABLE}"
    row = {
        "filename": filename,
        "source_name": SOURCE_NAME,
        "row_count": row_count,
        "loaded_at": datetime.now(timezone.utc).isoformat(),
    }
    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        raise RuntimeError(f"Failed to log {filename}: {errors}")


def run():
    already_loaded = get_already_loaded_files()
    all_files = sorted(os.listdir(INPUT_DIR))

    if not all_files:
        print("No files found in input directory.")
        return

    new_count = 0
    for filename in all_files:
        if filename in already_loaded:
            print(f"SKIP  {filename} (already loaded)")
            continue

        filepath = os.path.join(INPUT_DIR, filename)

        header, _ = read_csv_rows(filepath)
        new_columns = detect_new_columns(header)

        if new_columns:
            log_schema_change(filename, new_columns)

        row_count = load_file_to_bigquery(filename, filepath, new_columns)
        log_ingested_file(filename, row_count)
        print(f"LOADED {filename} ({row_count} rows)")
        new_count += 1

    print(f"\nDone. {new_count} new file(s) loaded, "
          f"{len(all_files) - new_count} skipped (already loaded).")


if __name__ == "__main__":
    run()