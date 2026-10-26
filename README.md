# Idempotent Multi-Source Data Pipeline (Medallion Architecture)

I built this project to practise the parts of data engineering that usually only show up when something goes wrong in production: rerunning a job that half failed, reloading data that turned out to be bad, rows that arrive late, and sources that change shape without warning.

So instead of one clean source, the pipeline pulls from four messy ones, each with its own way of delivering data and its own failure modes, lands them in BigQuery, and models them with dbt. Every guarantee in this README was tested against real runs, and the numbers in the "What I tested" section come from those runs.

The project is still in progress. Ingestion, the dbt models and the backfill process are done and tested. Airflow is set up in Docker and the DAG is the next piece.

---

## What the pipeline does

```
CSV files ─────────────┐
Order-status API ──────┤
Legacy customers DB ───┼──▶  raw (bronze)  ──▶  staging (silver)  ──▶  marts (gold)
Modern customers DB ───┘      BigQuery            dbt                   dbt
```

| Source | Simulates | Deliberate problems built in |
|---|---|---|
| CSV files | a partner dropping order files on a schedule | late rows, exact duplicate rows, a new column (`payment_method`) appearing after file 10 |
| Order-status API (FastAPI) | a live, paginated event API that keeps producing data | backdated events (late data), a new field (`carrier_name`) appearing after event 80, cursor pagination |
| Legacy customer DB (Postgres) | an old system: `full_name`, `signup_date` | inserts and updates every 5 seconds |
| Modern customer DB (Postgres) | a newer system: `first_name`/`last_name`, `created_at` | same customers as legacy sometimes, under different IDs |

The four sources are unrelated business domains, so the gold layer has one mart per domain rather than one joined model.

## Stack

Python, FastAPI, PostgreSQL (Docker), BigQuery, dbt Core, Airflow (Docker, in progress).

---

## The layers

**Raw (bronze).** Data exactly as it arrived. Duplicates, late rows and old versions of customer records all stay here. Raw is deliberately "dumb": it never fixes anything, so it can always be used to rebuild what comes after it.

**Staging (silver).** One incremental model per raw table. Cleans, renames and deduplicates:

| Model | Grain | Incremental on |
|---|---|---|
| `stg_csv_orders` | one row per `order_id` | `ingested_at` |
| `stg_api_order_status_events` | one row per `event_id` | `ingested_at` |
| `stg_legacy_customers` | one row per customer account, latest version | `updated_at` |
| `stg_modern_customers` | one row per customer account, latest version | `updated_at` |

**Marts (gold).** Business-facing, one per domain, all incremental:

| Mart | What it answers |
|---|---|
| `fct_orders` | sales: one row per order, with the real order time so late orders land on the right day |
| `fct_order_status_events` | fulfilment: one row per status change, with `changed_by_type` (system or agent) |
| `dim_customers` | one row per real person across both customer systems, matched by email |

Every mart has a `computed_at` column: the time the row was added or last changed. Coming from an analyst background, I know dashboard users always ask "when did this data last update?", and `max(computed_at)` answers that honestly. Because the marts are incremental, it only moves when records really were added or changed, not every time the job runs.

Each source's staging and mart models share a dbt tag (`csv_orders`, `api_events`, `db_customers`), so a source can be built, or rebuilt, on its own.

---

## How each problem is handled

### Safe reruns (idempotency)

| Source | How a rerun is made safe |
|---|---|
| CSV | a load log records each file after it loads; a file already in the log is skipped |
| API | a checkpoint on `sequence` means each run only asks for new events; an event-id log is a safety net |
| Databases | a checkpoint on `updated_at` means each run only pulls changed rows |

One decision I'd defend in any review: **the checkpoint or log is always written after the data, never before.** If a run crashes between the two, the worst case is a duplicate on the next run, which staging removes. Writing the checkpoint first would mean a crash silently loses data, which is much worse.

That also means raw is "at least once", not exactly once. I don't claim raw itself is idempotent for the database sources; idempotency is guaranteed end to end, with dbt staging doing the deduplication.

### Late-arriving data

Every incremental filter uses **when a row arrived**, never when the event happened. A late row has an old event time but a new arrival time, so it's always picked up, and in the marts it lands on the day it actually happened.

### Schema drift

New columns are detected by comparing incoming data with the expected schema, logged to an audit table (`csv_schema_change_log`, `api_schema_change_log`), and only then added to the raw table. Any other change (a removed column, a changed type) fails the load loudly instead of corrupting data. Staging and marts list their columns explicitly, so a new raw column only reaches them when someone deliberately adds it.

### Backfills

A backfill doesn't stop at raw. If rows are deleted from raw and not reloaded, they must disappear downstream too, and reloaded rows must flow all the way to the marts. Incremental runs can't do the first part (they only add or replace, never delete), so the runbook ends with a full rebuild of that source's models:

| Source | 1. Clear the slice in raw | 2. Reload | 3. Rebuild downstream |
|---|---|---|---|
| CSV | delete the file's rows (by `source_file`) and its log row | `python3 ingestion/ingest_csv.py` | `dbt build --full-refresh -s tag:csv_orders` |
| API | delete the sequence range from the log, then from raw | `python3 ingestion/ingest_api.py --backfill-since-sequence N --backfill-until-sequence M` | `dbt build --full-refresh -s tag:api_events` |
| Databases | delete the `updated_at` window from raw | `python3 ingestion/ingest_db.py --source legacy --backfill-since ... --backfill-until ...` | `dbt build --full-refresh -s tag:db_customers` |

Backfill runs never move the normal checkpoint.

---

## What I tested

| Test | Result |
|---|---|
| CSV staging dedup | 713 raw rows → 708 orders (5 duplicate rows removed) |
| CSV late rows kept | 12 of 12 late rows reached staging |
| API staging | 255 events → 255, all 41 late (backdated) events kept |
| Customer staging | legacy 176 → 163 accounts, modern 166 → 151, one row per account |
| Incremental runs | new data added correctly; rerun with nothing new changed nothing |
| CSV backfill, end to end | delete a file from raw → full refresh removed its 30 orders from staging and marts (708 → 678) → reload → full refresh restored them, duplicate removed again |
| API backfill (raw) | deleted and reloaded sequences 30 to 40: 11 events, no duplicates, checkpoint untouched |
| Database backfill (raw) | 48 deleted rows restored with `--backfill-since` / `--backfill-until` |
| Empty-table recovery | deleted all API staging rows, normal run reloaded them |

---

## Known limitations

- **Full refresh doesn't scale to very large tables.** At scale I'd partition the marts by date and use dbt's `insert_overwrite` strategy, which rewrites only the affected days. Unlike a merge, it removes rows that are no longer in the source for those days, so a backfill wouldn't need a full rebuild.
- **New columns are stored as text.** When a source adds a column, ingestion adds it to the raw table as text, whatever kind of value it really holds, because it can't safely guess the type of a column it has never seen. Both new columns in this project are text anyway, but a new number or date column would need converting to the right type in staging before use.
- **A full refresh resets `computed_at`** for every row in that mart. After a backfill, "last updated" shows the backfill time, which I think is fair, since the data did change.

---

## Running it

**Prerequisites:** Docker, Python 3.11+, a GCP project with BigQuery, and a service account key.

```bash
git clone https://github.com/Oreoluwa100/idempotent-multi-source-pipeline.git
cd idempotent-multi-source-pipeline
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` in the project root (it's gitignored):

```
GCP_PROJECT_ID=your-project-id
LEGACY_DB_HOST=localhost
LEGACY_DB_PORT=5434
LEGACY_DB_NAME=legacy_customers
LEGACY_DB_USER=postgres
LEGACY_DB_PASSWORD=postgres
MODERN_DB_HOST=localhost
MODERN_DB_PORT=5435
MODERN_DB_NAME=modern_customers
MODERN_DB_USER=postgres
MODERN_DB_PASSWORD=postgres
MOCK_API_URL=http://localhost:8000
```

Put the service account key in `.secrets/gcp-key.json` and export it:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.secrets/gcp-key.json"
```

Start the two customer databases:

```bash
docker run -d --name legacy_db -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=legacy_customers -p 5434:5432 postgres:15
docker run -d --name modern_db -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=modern_customers -p 5435:5432 postgres:15
```

Start the sources (separate terminals):

```bash
uvicorn data_generation.mock_api_source:app --host 0.0.0.0 --port 8000
python3 data_generation/generate_csv_source.py
python3 data_generation/generate_db_sources.py
```

Ingest:

```bash
python3 ingestion/ingest_csv.py
python3 ingestion/ingest_api.py
python3 ingestion/ingest_db.py
```

Build the models (`~/.dbt/profiles.yml` needs a `dev` and a `prod` target for BigQuery):

```bash
cd dbt_project
dbt build                          # everything, dev target
dbt build -s tag:csv_orders        # one source
dbt build --target prod            # clean staging/marts datasets
```

---

## Repo layout

```
data_generation/   the four fake sources
ingestion/         one ingestion script per source type
dbt_project/       staging and mart models, tests, schema macro
airflow/           Dockerfile and docker-compose for Airflow
dags/              Airflow DAG (in progress)
docs/              architecture notes (in progress)
```

---

## What's next

1. **Airflow DAG**: ingestion then dbt for each source as independent branches, every 10 minutes.
2. **10x volume test**: raise every source's volume tenfold, find what breaks, and document the fixes.
