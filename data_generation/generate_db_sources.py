"""
Simulates two live, independently-evolving "on-prem" customer databases:
- legacy_db (Postgres, port 5434): older system, combined full_name, signup_date only
- modern_db (Postgres, port 5435): newer system, split first/last name, created_at

Every GENERATE_INTERVAL_SECONDS, each database either inserts a new
customer (70% chance) or updates an existing one (30% chance).

Deliberate cross-system overlap: occasionally, a new customer inserted
into one system reuses the (name, email) identity of an existing
customer from the OTHER system — simulating the same real person
existing in both systems, under different IDs and structures. This is
the entity-matching problem the dbt layer will resolve later.

Run with:
    python3 data_generation/generate_db_sources.py
"""

import os
import random
import threading
import time
import uuid
from datetime import datetime, timezone

import psycopg2

from dotenv import load_dotenv
load_dotenv()

GENERATE_INTERVAL_SECONDS = 5
UPDATE_CHANCE = 0.3
CROSS_SYSTEM_OVERLAP_CHANCE = 0.2

LEGACY_CONN = dict(
    host=os.environ["LEGACY_DB_HOST"],
    port=int(os.environ["LEGACY_DB_PORT"]),
    dbname=os.environ["LEGACY_DB_NAME"],
    user=os.environ["LEGACY_DB_USER"],
    password=os.environ["LEGACY_DB_PASSWORD"],
)
MODERN_CONN = dict(
    host=os.environ["MODERN_DB_HOST"],
    port=int(os.environ["MODERN_DB_PORT"]),
    dbname=os.environ["MODERN_DB_NAME"],
    user=os.environ["MODERN_DB_USER"],
    password=os.environ["MODERN_DB_PASSWORD"],
)

FIRST_NAMES = ["Ade", "Chioma", "Tunde", "Ngozi", "Femi", "Amara", "Bola", "Ifeoma"]
LAST_NAMES = ["Okafor", "Adeyemi", "Balogun", "Eze", "Uche", "Okonkwo", "Bello"]
STATUSES = ["active", "inactive", "suspended"]

lock = threading.Lock()
# Each system keeps its own pool of (first, last, email) identities it has created.
legacy_identities = []
modern_identities = []
legacy_known_ids = []
modern_known_ids = []


def random_identity():
    """Generates a brand-new (first, last, email) identity."""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    email = f"{first.lower()}.{last.lower()}{random.randint(1,999)}@example.com"
    return first, last, email


def get_connection(conn_params):
    return psycopg2.connect(**conn_params)


def legacy_insert(cur):
    with lock:
        if modern_identities and random.random() < CROSS_SYSTEM_OVERLAP_CHANCE:
            first, last, email = random.choice(modern_identities)  # from the OTHER system
        else:
            first, last, email = random_identity()
            legacy_identities.append((first, last, email))

    customer_id = f"L-{uuid.uuid4().hex[:8]}"
    signup_date = datetime.now(timezone.utc).date()
    status = random.choice(STATUSES)
    now = datetime.now(timezone.utc)

    cur.execute(
        """
        INSERT INTO customers (customer_id, full_name, email, signup_date, account_status, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (customer_id, f"{first} {last}", email, signup_date, status, now),
    )

    with lock:
        legacy_known_ids.append(customer_id)

    print(f"[legacy_db] INSERT {customer_id} ({first} {last}, {email})")


def legacy_update(cur):
    with lock:
        if not legacy_known_ids:
            return
        customer_id = random.choice(legacy_known_ids)

    new_status = random.choice(STATUSES)
    now = datetime.now(timezone.utc)

    cur.execute(
        """
        UPDATE customers
        SET account_status = %s, updated_at = %s
        WHERE customer_id = %s
        """,
        (new_status, now, customer_id),
    )
    print(f"[legacy_db] UPDATE {customer_id} -> status={new_status}")


def modern_insert(cur):
    with lock:
        if legacy_identities and random.random() < CROSS_SYSTEM_OVERLAP_CHANCE:
            first, last, email = random.choice(legacy_identities)  # from the OTHER system
        else:
            first, last, email = random_identity()
            modern_identities.append((first, last, email))

    customer_id = f"M-{uuid.uuid4().hex[:8]}"
    created_at = datetime.now(timezone.utc)
    status = random.choice(STATUSES)

    cur.execute(
        """
        INSERT INTO customers (id, first_name, last_name, email_address, created_at, status, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (customer_id, first, last, email, created_at, status, created_at),
    )

    with lock:
        modern_known_ids.append(customer_id)

    print(f"[modern_db] INSERT {customer_id} ({first} {last}, {email})")


def modern_update(cur):
    with lock:
        if not modern_known_ids:
            return
        customer_id = random.choice(modern_known_ids)

    new_status = random.choice(STATUSES)
    now = datetime.now(timezone.utc)

    cur.execute(
        """
        UPDATE customers
        SET status = %s, updated_at = %s
        WHERE id = %s
        """,
        (new_status, now, customer_id),
    )
    print(f"[modern_db] UPDATE {customer_id} -> status={new_status}")


def run_generator(conn_params, insert_fn, update_fn, label):
    conn = get_connection(conn_params)
    conn.autocommit = True
    cur = conn.cursor()

    print(f"[{label}] Generator started.")
    while True:
        time.sleep(GENERATE_INTERVAL_SECONDS)
        if random.random() < UPDATE_CHANCE:
            update_fn(cur)
        else:
            insert_fn(cur)


def main():
    legacy_thread = threading.Thread(
        target=run_generator,
        args=(LEGACY_CONN, legacy_insert, legacy_update, "legacy_db"),
        daemon=True,
    )
    modern_thread = threading.Thread(
        target=run_generator,
        args=(MODERN_CONN, modern_insert, modern_update, "modern_db"),
        daemon=True,
    )

    legacy_thread.start()
    modern_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping generators.")


if __name__ == "__main__":
    main()