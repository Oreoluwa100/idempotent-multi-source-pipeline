"""
Pulls customer records from two live Postgres "on-prem" databases
(legacy_db, modern_db) and lands them into their respective raw tables
in BigQuery.

Checkpoint-based incremental pulls: each source tracks the max updated_at
successfully pulled so far (raw.db_ingestion_checkpoint), and normal runs
only query rows with updated_at > checkpoint — avoiding a full table scan
every run.

No dedup log: rows are appended to raw as-is, including the same customer
appearing multiple times across pulls if they were updated again. This is
intentional — raw is a faithful, append-only log of what was pulled at
each point in time. Collapsing to "current state per customer" is dbt
staging's job (QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY
updated_at DESC) = 1).

No schema drift detection here: these sources have fixed, known schemas
(unlike the event-stream sources). Structural differences between legacy
and modern systems are a reconciliation problem for dbt, not something
ingestion detects or reacts to live.

Backfill: explicit --backfill-since / --backfill-until flags (on
updated_at) override the checkpoint for a single run, without advancing
it afterward.

One script, two sources: all source-specific detail (connection, target
table, columns) lives in the SOURCES config dict. Every function below
operates generically on whatever source config it's given.
"""

import argparse
import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

# ---- Config ----
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = "raw"
CHECKPOINT_TABLE = "db_ingestion_checkpoint"

SOURCES = {
    "legacy": {
        "source_name": "legacy_db",
        "conn": dict(
            host=os.environ["LEGACY_DB_HOST"],
            port=int(os.environ["LEGACY_DB_PORT"]),
            dbname=os.environ["LEGACY_DB_NAME"],
            user=os.environ["LEGACY_DB_USER"],
            password=os.environ["LEGACY_DB_PASSWORD"],
        ),
        "target_table": "legacy_customers",
        "id_column": "customer_id",
        "select_columns": [
            "customer_id", "full_name", "email",
            "signup_date", "account_status", "updated_at",
        ],
    },
    "modern": {
        "source_name": "modern_db",
        "conn": dict(
            host=os.environ["MODERN_DB_HOST"],
            port=int(os.environ["MODERN_DB_PORT"]),
            dbname=os.environ["MODERN_DB_NAME"],
            user=os.environ["MODERN_DB_USER"],
            password=os.environ["MODERN_DB_PASSWORD"],
        ),
        "target_table": "modern_customers",
        "id_column": "id",
        "select_columns": [
            "id", "first_name", "last_name",
            "email_address", "created_at", "status", "updated_at",
        ],
    },
}

client = bigquery.Client(project=PROJECT_ID)


def get_checkpoint(source_name: str):
    """Returns the saved checkpoint (max updated_at loaded so far), or None."""
    query = f"""
        SELECT last_updated_at
        FROM `{PROJECT_ID}.{DATASET}.{CHECKPOINT_TABLE}`
        WHERE source_name = @source_name
        ORDER BY checkpoint_saved_at DESC
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("source_name", "STRING", source_name)]
    )
    results = list(client.query(query, job_config=job_config).result())
    if not results:
        return None
    return results[0]["last_updated_at"]


def save_checkpoint(source_name: str, new_checkpoint: datetime):
    """Records the new checkpoint after a successful normal run."""
    table_ref = f"{PROJECT_ID}.{DATASET}.{CHECKPOINT_TABLE}"
    row = {
        "source_name": source_name,
        "last_updated_at": new_checkpoint.isoformat(),
        "checkpoint_saved_at": datetime.now(timezone.utc).isoformat(),
    }
    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        raise RuntimeError(f"Failed to save checkpoint for {source_name}: {errors}")
    print(f"  Checkpoint for {source_name} updated to {new_checkpoint.isoformat()}")


def fetch_rows(source_config: dict, since, until) -> list:
    """Queries the source's live Postgres database for rows updated in the given window."""
    columns = ", ".join(source_config["select_columns"])
    query = f"SELECT {columns} FROM customers WHERE updated_at > %s"
    params = [since] if since else [datetime(1970, 1, 1, tzinfo=timezone.utc)]

    if until:
        query += " AND updated_at <= %s"
        params.append(until)

    query += " ORDER BY updated_at ASC"

    conn = psycopg2.connect(**source_config["conn"])
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query, params)
        rows = cur.fetchall()
    finally:
        conn.close()

    return [dict(row) for row in rows]


def insert_rows_to_bigquery(source_config: dict, rows: list):
    """Inserts rows into the source's raw table, converting date/datetime values and tagging ingested_at."""
    table_ref = f"{PROJECT_ID}.{DATASET}.{source_config['target_table']}"
    now = datetime.now(timezone.utc).isoformat()

    bq_rows = []
    for row in rows:
        bq_row = {}
        for key, value in row.items():
            if hasattr(value, "isoformat"):
                bq_row[key] = value.isoformat()
            else:
                bq_row[key] = value
        bq_row["ingested_at"] = now
        bq_rows.append(bq_row)

    errors = client.insert_rows_json(table_ref, bq_rows)
    if errors:
        raise RuntimeError(f"Failed to insert rows into {table_ref}: {errors}")


def run_source(source_key: str, backfill_since=None, backfill_until=None):
    """Runs the full ingestion flow for one source: checkpoint, fetch, insert, save."""
    source_config = SOURCES[source_key]
    source_name = source_config["source_name"]
    is_backfill = backfill_since is not None

    if is_backfill:
        print(f"[{source_name}] BACKFILL MODE: since={backfill_since}, until={backfill_until}")
        since = backfill_since
        until = backfill_until
    else:
        since = get_checkpoint(source_name)
        until = None
        print(f"[{source_name}] Normal run. Checkpoint: {since if since else '(none — first run)'}")

    rows = fetch_rows(source_config, since, until)
    print(f"[{source_name}] Fetched {len(rows)} row(s) for this window.")

    if rows:
        insert_rows_to_bigquery(source_config, rows)
        print(f"[{source_name}] Inserted {len(rows)} row(s) into raw.{source_config['target_table']}.")

        max_updated_at = max(row["updated_at"] for row in rows)
        if not is_backfill:
            save_checkpoint(source_name, max_updated_at)
    else:
        print(f"[{source_name}] Nothing new.")


def run(backfill_since=None, backfill_until=None, only_source=None):
    """Runs one or both sources, depending on only_source."""
    sources_to_run = [only_source] if only_source else list(SOURCES.keys())
    for source_key in sources_to_run:
        run_source(source_key, backfill_since=backfill_since, backfill_until=backfill_until)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["legacy", "modern"], help="run only this source")
    parser.add_argument("--backfill-since", help="ISO datetime: start of backfill window")
    parser.add_argument("--backfill-until", help="ISO datetime: end of backfill window (optional)")
    args = parser.parse_args()

    since_dt = datetime.fromisoformat(args.backfill_since) if args.backfill_since else None
    until_dt = datetime.fromisoformat(args.backfill_until) if args.backfill_until else None

    run(backfill_since=since_dt, backfill_until=until_dt, only_source=args.source)