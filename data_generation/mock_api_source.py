"""
A local mock API simulating a live, continuously-updating order-status
events API. New events are appended in the background for as long as
this server runs.

Each event carries a `sequence` number reflecting its true creation
order — this is separate from status_changed_at, which can be deliberately
backdated to simulate late-arriving data. Incremental filtering
(since_sequence) is based on sequence, not status_changed_at, so a
late-arriving event is never silently and permanently excluded just
because it claims an early timestamp.

Supports cursor-based pagination and since_sequence-based filtering.

Deliberately includes late-arriving events and schema evolution
(carrier_name appears once event index reaches SCHEMA_CHANGE_AFTER_EVENT_INDEX).

Run with:
    uvicorn data_generation.mock_api_source:app --reload --port 8000

Endpoints:
    GET /events?limit=20&cursor=<event_id>&since_sequence=<int>
"""

import asyncio
import random
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, Query

# ---- Config ----
NEW_EVENT_INTERVAL_SECONDS = 2
SCHEMA_CHANGE_AFTER_EVENT_INDEX = 80
LATE_DATA_CHANCE = 0.15

STATUSES = ["placed", "shipped", "delivered", "cancelled"]
UPDATED_BY_POOL = ["system_auto", "agent_012", "agent_034", "agent_099"]
CARRIERS = ["DHL", "UPS", "FedEx", "GIG Logistics"]

ALL_EVENTS = []
_next_index = 0


def generate_event(index: int) -> dict:
    """Generates one fake event, given its true creation-order position."""
    now = datetime.now(timezone.utc)

    event_time = now
    if random.random() < LATE_DATA_CHANCE:
        event_time -= timedelta(hours=random.randint(1, 5))

    event = {
        "event_id": f"evt_{index:05d}",
        "sequence": index,  # true creation order — never backdated
        "order_id": str(uuid.uuid4()),
        "status": random.choice(STATUSES),
        "status_changed_at": event_time.isoformat(),
        "updated_by": random.choice(UPDATED_BY_POOL),
    }

    if index >= SCHEMA_CHANGE_AFTER_EVENT_INDEX:
        event["carrier_name"] = random.choice(CARRIERS)

    return event


async def generate_new_events_periodically():
    """Background task: appends one new event every NEW_EVENT_INTERVAL_SECONDS."""
    global _next_index
    while True:
        await asyncio.sleep(NEW_EVENT_INTERVAL_SECONDS)
        new_event = generate_event(_next_index)
        ALL_EVENTS.append(new_event)
        _next_index += 1
        print(f"[API] New event generated: {new_event['event_id']} "
              f"(sequence {new_event['sequence']}) at {new_event['status_changed_at']}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(generate_new_events_periodically())
    yield
    task.cancel()


app = FastAPI(title="Mock Order Status Events API", lifespan=lifespan)


@app.get("/events")
def get_events(
    limit: int = Query(20, ge=1, le=100),
    cursor: Optional[str] = Query(None, description="event_id to resume after"),
    since_sequence: Optional[int] = Query(None, description="only return events with sequence greater than this"),
):
    """
    Cursor-based pagination, with optional since_sequence filtering.
    since_sequence filters by true creation order, not status_changed_at,
    so late-arriving events are never silently excluded.
    """
    events = ALL_EVENTS

    if since_sequence is not None:
        events = [e for e in events if e["sequence"] > since_sequence]

    start_index = 0
    if cursor:
        for i, e in enumerate(events):
            if e["event_id"] == cursor:
                start_index = i + 1
                break

    page = events[start_index : start_index + limit]
    next_index = start_index + limit
    next_cursor = events[next_index - 1]["event_id"] if next_index < len(events) else None

    return {
        "data": page,
        "next_cursor": next_cursor,
    }


@app.get("/")
def root():
    return {
        "message": "Mock Order Status Events API. See /docs for details.",
        "current_event_count": len(ALL_EVENTS),
    }