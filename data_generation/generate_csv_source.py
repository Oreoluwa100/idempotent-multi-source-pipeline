"""
Simulates CSV file source every 2 minutes.
Each run produces one CSV file representing a time window of order events.

Deliberately includes:
- Late-arriving rows (event_timestamp from an earlier window)
- Schema evolution (a new column appears after N files)
- Occasional duplicate row (to test idempotent loading downstream)
"""

import csv
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

# ---- Config ----
OUTPUT_DIR = "data_generation/output/csv_source"
INTERVAL_MINUTES = 2
SCHEMA_CHANGE_AFTER_N_FILES = 10  # after this many files, add payment_method column
LATE_DATA_CHANCE = 0.2            # 20% of files include a late row
DUPLICATE_CHANCE = 0.1            # 10% of files include a duplicate row
ROWS_PER_FILE = (10, 30)          # random range of rows per file

CUSTOMER_POOL = [f"cust_{i:04d}" for i in range(1, 201)]
PRODUCT_CATEGORIES = ["Sport wears", "Gym wears", "Lounge wears", "Party wears"]
PAYMENT_METHODS = ["card", "transfer", "wallet"]


def generate_order_row(event_time: datetime, ingested_at: datetime, include_payment_method: bool) -> dict:
    row = {
        "order_id": str(uuid.uuid4()),
        "customer_id": random.choice(CUSTOMER_POOL),
        "product_category": random.choice(PRODUCT_CATEGORIES),
        "amount": round(random.uniform(5.0, 500.0), 2),
        "event_timestamp": event_time.isoformat(),
        "ingested_at": ingested_at.isoformat(),
    }
    if include_payment_method:
        row["payment_method"] = random.choice(PAYMENT_METHODS)
    return row


def generate_file(file_index: int, window_start: datetime) -> str:
    include_payment_method = file_index >= SCHEMA_CHANGE_AFTER_N_FILES
    now = datetime.now(timezone.utc)

    num_rows = random.randint(*ROWS_PER_FILE)
    rows = []

    for _ in range(num_rows):
        # normal row: event happened within this window
        offset_seconds = random.randint(0, INTERVAL_MINUTES * 60 - 1)
        event_time = window_start + timedelta(seconds=offset_seconds)
        rows.append(generate_order_row(event_time, now, include_payment_method))

    # occasionally inject a late row (event_timestamp from an earlier window)
    if random.random() < LATE_DATA_CHANCE:
        late_offset_minutes = random.randint(INTERVAL_MINUTES, INTERVAL_MINUTES * 3)
        late_event_time = window_start - timedelta(minutes=late_offset_minutes)
        rows.append(generate_order_row(late_event_time, now, include_payment_method))

    # occasionally inject a duplicate row
    if random.random() < DUPLICATE_CHANCE and rows:
        rows.append(dict(rows[0]))  # exact duplicate of an existing row

    # write file
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = f"source1_{window_start.strftime('%Y-%m-%dT%H-%M')}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)

    fieldnames = ["order_id", "customer_id", "product_category", "amount", "event_timestamp", "ingested_at"]
    if include_payment_method:
        fieldnames.append("payment_method")

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"[{now.isoformat()}] Wrote {len(rows)} rows to {filepath} "
          f"(schema_evolved={include_payment_method})")
    return filepath


def run(num_files: int = None, skip_sleep: bool = False):
    """
    Generates one file every INTERVAL_MINUTES.

    num_files: how many files to generate. None = run forever.
    skip_sleep: if True, don't wait between files (for fast local testing).
    """
    file_index = 0
    window_start = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    while num_files is None or file_index < num_files:
        generate_file(file_index, window_start)
        file_index += 1
        window_start += timedelta(minutes=INTERVAL_MINUTES)
        if not skip_sleep:
            time.sleep(INTERVAL_MINUTES * 60)


if __name__ == "__main__":
    if "--test" in sys.argv:
        # Instantly generate 15 files, no waiting — lets you see schema
        # evolution (kicks in after file 10) without sitting around.
        run(num_files=15, skip_sleep=True)
    else:
        # Real-time mode: one file every INTERVAL_MINUTES, forever.
        run()