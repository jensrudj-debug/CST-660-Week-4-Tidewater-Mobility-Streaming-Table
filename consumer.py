"""Micro-batch streaming consumer for Tidewater Mobility trip events.

Reads events.jsonl in batches of BATCH_SIZE lines, as if tailing a live
stream, and counts trips per zone in tumbling 15-minute windows two ways:

  * by event_time (when the trip actually happened), using a watermark and
    allowed lateness to decide when a window is final, and
  * by processing_time (when the event reached us), the naive comparison.

Events that arrive after their event-time window has closed are routed to a
late_events side output instead of being dropped.

At the end of every batch the results are appended to Delta Lake tables under
lake/: trips_raw, zone_counts_event_time, zone_counts_processing_time and
late_events. Appends use schema merging, so the surge_multiplier column that
appears on day 4 is added to the tables instead of failing the write.

Progress is saved to a checkpoint after every batch, so a run stopped with
--through-day can be resumed by running the script again. Writes are
idempotent: if a run crashes partway through writing a batch, the next run
replays that batch and each table skips it if it already has it, so no rows
are duplicated (see "Idempotent writes" below).

Usage:
    python consumer.py --through-day 3   # process the stream up to the end of day 3
    python consumer.py                   # resume and process the rest
    python consumer.py --reset           # forget the checkpoint and start over
"""

import argparse
import json
from collections import Counter
from datetime import datetime, timedelta
from itertools import islice
from pathlib import Path

import pyarrow as pa
from deltalake import CommitProperties, DeltaTable, Transaction, write_deltalake

from generator import OUTPUT_PATH as EVENTS_PATH, START_DATE

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BATCH_SIZE = 500
WINDOW_SIZE = timedelta(minutes=15)
WATERMARK_DELAY = timedelta(minutes=2)
ALLOWED_LATENESS = timedelta(minutes=10)
LAKE_DIR = Path("lake")
CHECKPOINT_PATH = LAKE_DIR / "_checkpoints" / "consumer_state.json"
MAX_LATE_EXAMPLES = 5  # late events printed per batch

TRIPS_RAW_TABLE = LAKE_DIR / "trips_raw"
EVENT_COUNTS_TABLE = LAKE_DIR / "zone_counts_event_time"
PROCESSING_COUNTS_TABLE = LAKE_DIR / "zone_counts_processing_time"
LATE_EVENTS_TABLE = LAKE_DIR / "late_events"


# ---------------------------------------------------------------------------
# How the watermark works (read this first)
# ---------------------------------------------------------------------------
# Events do not arrive in event_time order: a phone in the harbor tunnel can
# hold a trip for minutes before sending it. So when can we say "the 10:00-10:15
# window is finished, nobody will send us more trips for it"? We can never be
# 100% sure, so we pick a rule. That rule has two parts.
#
# 1. The WATERMARK is the stream's estimate of "how far event time has got".
#
#        watermark = (latest event_time seen so far) - WATERMARK_DELAY
#
#    If the newest trip we have seen happened at 10:20, the watermark is 10:18.
#    It only ever moves forward: a late event with an old event_time does not
#    pull it back, because max() ignores smaller values.
#
# 2. ALLOWED LATENESS is extra grace time a window stays open after the
#    watermark passes its end. A window closes once
#
#        window_end + ALLOWED_LATENESS <= watermark
#
#    For the 10:00-10:15 window that means the watermark must reach 10:25,
#    which happens when we see a trip with event_time 10:27 or later.
#
# Together: a trip is accepted as long as it arrives before the stream has seen
# a trip from 12 minutes (2 + 10) past the end of its window. Tunnel delays of
# 2-8 minutes fit comfortably inside that; delays of 20+ minutes do not.
#
# When a window closes we emit its final count and delete it from memory. Any
# event that shows up later for that window is LATE: it goes to the late_events
# side output with how far behind the watermark it was. It is never dropped,
# so nothing silently disappears from the counts.
#
# The watermark advances after every event, not just once per batch. A batch of
# 500 lines covers several hours of the stream here, so a once-per-batch
# watermark would accept almost anything that arrives within the same batch and
# make the lateness policy depend on batch size rather than on the rule above.
# Closed windows are still collected and emitted at the end of each batch.


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def fmt_ts(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def window_start(ts: datetime) -> datetime:
    """Start of the tumbling window that contains ts (e.g. 10:07 -> 10:00)."""
    size = WINDOW_SIZE.total_seconds()
    return datetime.fromtimestamp(ts.timestamp() // size * size, tz=ts.tzinfo)


def window_closes_at(start: datetime) -> datetime:
    """Watermark value at which the window starting at `start` closes."""
    return start + WINDOW_SIZE + ALLOWED_LATENESS


# ---------------------------------------------------------------------------
# State (everything needed to resume a stopped run)
# ---------------------------------------------------------------------------
class State:
    def __init__(self):
        self.offset = 0  # lines of events.jsonl already processed
        self.batch_number = 0
        self.max_event_time = None
        self.max_processing_time = None
        # Open windows: (window_start, zone) -> trip count
        self.event_windows = Counter()
        self.processing_windows = Counter()
        self.total_late = 0
        # Set while a batch is being written: the offset that batch ends at.
        # If the run crashes mid-write, the next run replays exactly that range.
        self.pending_end_offset = None

    @property
    def watermark(self):
        if self.max_event_time is None:
            return None  # nothing seen yet, so nothing can be late
        return self.max_event_time - WATERMARK_DELAY

    def to_json(self) -> dict:
        def windows(counter):
            return [[start.isoformat(), zone, n] for (start, zone), n in sorted(counter.items())]

        return {
            "offset": self.offset,
            "batch_number": self.batch_number,
            "max_event_time": self.max_event_time and self.max_event_time.isoformat(),
            "max_processing_time": self.max_processing_time and self.max_processing_time.isoformat(),
            "event_windows": windows(self.event_windows),
            "processing_windows": windows(self.processing_windows),
            "total_late": self.total_late,
            "pending_end_offset": self.pending_end_offset,
        }

    @classmethod
    def from_json(cls, data: dict) -> "State":
        def windows(rows):
            return Counter({(parse_ts(start), zone): n for start, zone, n in rows})

        state = cls()
        state.offset = data["offset"]
        state.batch_number = data["batch_number"]
        state.max_event_time = data["max_event_time"] and parse_ts(data["max_event_time"])
        state.max_processing_time = data["max_processing_time"] and parse_ts(data["max_processing_time"])
        state.event_windows = windows(data["event_windows"])
        state.processing_windows = windows(data["processing_windows"])
        state.total_late = data["total_late"]
        state.pending_end_offset = data["pending_end_offset"]
        return state


def load_state() -> State:
    if CHECKPOINT_PATH.exists():
        return State.from_json(json.loads(CHECKPOINT_PATH.read_text()))
    return State()


def save_state(state: State) -> None:
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state.to_json()))
    tmp.replace(CHECKPOINT_PATH)


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------
def process_batch(state: State, events: list[dict]):
    """Update state with one micro-batch.

    Returns (closed_event_windows, closed_processing_windows, late_events),
    where each closed_* is a list of (window_start, zone, count).
    """
    late_events = []

    for event in events:
        event_time = parse_ts(event["event_time"])
        processing_time = parse_ts(event["processing_time"])
        zone = event["zone"]

        # --- Event-time count, guarded by the watermark ---------------------
        start = window_start(event_time)
        watermark = state.watermark
        if watermark is not None and window_closes_at(start) <= watermark:
            # This event's window already closed and its count was emitted.
            # Route it to the side output instead of dropping it.
            late_events.append({
                **event,
                "window_start": start.isoformat(),
                "window_end": (start + WINDOW_SIZE).isoformat(),
                "watermark": watermark.isoformat(),
                "lateness_seconds": round((watermark - event_time).total_seconds(), 3),
            })
        else:
            state.event_windows[(start, zone)] += 1

        # --- Processing-time count (naive: every event counts, nothing is late)
        state.processing_windows[(window_start(processing_time), zone)] += 1

        # --- Advance the watermark ------------------------------------------
        # Only a newer event_time moves it forward; late events never pull it back.
        if state.max_event_time is None or event_time > state.max_event_time:
            state.max_event_time = event_time
        if state.max_processing_time is None or processing_time > state.max_processing_time:
            state.max_processing_time = processing_time

    # --- Close and emit finished windows ------------------------------------
    # Event-time windows close once the watermark passes end + allowed lateness.
    closed_event = [
        (start, zone, n)
        for (start, zone), n in state.event_windows.items()
        if state.watermark is not None and window_closes_at(start) <= state.watermark
    ]
    for start, zone, _ in closed_event:
        del state.event_windows[(start, zone)]

    # Processing time only moves forward, so a processing-time window is final
    # as soon as the stream clock passes its end. No watermark is needed.
    closed_processing = [
        (start, zone, n)
        for (start, zone), n in state.processing_windows.items()
        if start + WINDOW_SIZE <= state.max_processing_time
    ]
    for start, zone, _ in closed_processing:
        del state.processing_windows[(start, zone)]

    state.total_late += len(late_events)
    return sorted(closed_event), sorted(closed_processing), late_events


# ---------------------------------------------------------------------------
# Writing to Delta Lake
# ---------------------------------------------------------------------------
TIMESTAMP = pa.timestamp("us", tz="UTC")
EVENT_FIELDS = [
    pa.field("trip_id", pa.string()),
    pa.field("zone", pa.string()),
    pa.field("event_time", TIMESTAMP),
    pa.field("processing_time", TIMESTAMP),
    pa.field("fare", pa.float64()),
]
SURGE_FIELD = pa.field("surge_multiplier", pa.float64())
LATE_FIELDS = EVENT_FIELDS + [
    pa.field("window_start", TIMESTAMP),
    pa.field("window_end", TIMESTAMP),
    pa.field("watermark", TIMESTAMP),
    pa.field("lateness_seconds", pa.float64()),
]
WINDOW_SCHEMA = pa.schema([
    pa.field("zone", pa.string()),
    pa.field("window_start", TIMESTAMP),
    pa.field("trip_count", pa.int64()),
    pa.field("batch_id", pa.int64()),
])


def events_to_arrow(rows: list[dict], fields: list[pa.Field], batch_id: int) -> pa.Table:
    """Build an Arrow table from event dicts.

    surge_multiplier is only included when at least one row has it, so the
    tables start without the column and gain it when day 4 arrives. Rows in
    the same batch that lack the key (e.g. late day-3 events) get null.
    """
    if any(SURGE_FIELD.name in row for row in rows):
        fields = fields + [SURGE_FIELD]
    columns = {}
    for field in fields:
        values = [row.get(field.name) for row in rows]
        if field.type == TIMESTAMP:
            values = [parse_ts(v) for v in values]
        columns[field.name] = pa.array(values, type=field.type)
    columns["batch_id"] = pa.array([batch_id] * len(rows), type=pa.int64())
    return pa.table(columns)


def windows_to_arrow(closed: list, batch_id: int) -> pa.Table:
    return pa.table({
        "zone": [zone for _, zone, _ in closed],
        "window_start": [start for start, _, _ in closed],
        "trip_count": [n for _, _, n in closed],
        "batch_id": [batch_id] * len(closed),
    }, schema=WINDOW_SCHEMA)


# Idempotent writes
# -----------------
# Each append is tagged with a Delta "application transaction": the pair
# (APP_ID, batch_id) is stored in the same commit as the data, so either both
# land or neither does. Before appending, we ask the table which batch_id it
# last committed for APP_ID. If it already has this batch, the append is
# skipped. Replaying a batch after a crash therefore never duplicates rows,
# even if the crash happened after some tables were written and not others.
APP_ID = "tidewater-consumer"


def append(table_path: Path, data: pa.Table, batch_id: int) -> bool:
    """Append data as batch_id. Returns False if the table already had it."""
    if not data.num_rows:
        return True
    uri = str(table_path)
    if DeltaTable.is_deltatable(uri):
        committed = DeltaTable(uri).transaction_version(APP_ID)
        if committed is not None and committed >= batch_id:
            return False
    # schema_mode="merge" lets an append add new columns (surge_multiplier)
    # to the table schema. Older rows read back as null for the new column.
    write_deltalake(
        uri, data, mode="append", schema_mode="merge",
        commit_properties=CommitProperties(app_transactions=[Transaction(APP_ID, batch_id)]),
    )
    return True


def write_batch(batch_id, events, closed_event, closed_processing, late_events) -> None:
    writes = [
        (TRIPS_RAW_TABLE, events_to_arrow(events, EVENT_FIELDS, batch_id)),
        (EVENT_COUNTS_TABLE, windows_to_arrow(closed_event, batch_id)),
        (PROCESSING_COUNTS_TABLE, windows_to_arrow(closed_processing, batch_id)),
        (LATE_EVENTS_TABLE, events_to_arrow(late_events, LATE_FIELDS, batch_id)),
    ]
    for table_path, data in writes:
        if not append(table_path, data, batch_id):
            print(f"  {table_path.name}: batch {batch_id} already committed, skipped")


# ---------------------------------------------------------------------------
# Reading the stream
# ---------------------------------------------------------------------------
def read_batches(start_offset: int, stop_at: datetime | None, replay_end: int | None):
    """Yield lists of up to BATCH_SIZE events, starting after start_offset.

    Stops before the first event whose processing_time is at or after stop_at,
    so a resumed run picks up exactly where this one left off.

    If replay_end is set, the first batch is exactly the lines up to
    replay_end, ignoring stop_at: it is a batch an earlier run started writing,
    and it must be rebuilt with the same boundaries to match what was written.
    """
    with open(EVENTS_PATH, encoding="utf-8") as f:
        batch = []
        for line_number, line in enumerate(islice(f, start_offset, None), start=start_offset + 1):
            event = json.loads(line)
            replaying = replay_end is not None and line_number <= replay_end
            if not replaying and stop_at is not None and parse_ts(event["processing_time"]) >= stop_at:
                break
            batch.append(event)
            if len(batch) == BATCH_SIZE or (replaying and line_number == replay_end):
                yield batch
                batch = []
        if batch:
            yield batch


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def print_batch_summary(state, events, closed_event, closed_processing, late_events):
    first_line = state.offset - len(events) + 1
    print(f"\n=== Batch {state.batch_number}: lines {first_line}-{state.offset} ===")
    print(f"processing_time {fmt_ts(parse_ts(events[0]['processing_time']))} -> "
          f"{fmt_ts(parse_ts(events[-1]['processing_time']))}")
    print(f"watermark       {fmt_ts(state.watermark)}  "
          f"(max event_time {fmt_ts(state.max_event_time)} - {WATERMARK_DELAY})")

    def span(closed):
        if not closed:
            return "none"
        first = min(s for s, _, _ in closed)
        last = max(s for s, _, _ in closed) + WINDOW_SIZE
        return f"{fmt_ts(first)} -> {fmt_ts(last)}"

    event_by_zone = Counter()
    for _, zone, n in closed_event:
        event_by_zone[zone] += n
    processing_by_zone = Counter()
    for _, zone, n in closed_processing:
        processing_by_zone[zone] += n

    print(f"closed event-time windows:      {len(closed_event):>3}  span {span(closed_event)}")
    print(f"closed processing-time windows: {len(closed_processing):>3}  span {span(closed_processing)}")
    print(f"  {'zone':<14}{'event-time':>12}{'processing-time':>17}")
    for zone in sorted(set(event_by_zone) | set(processing_by_zone)):
        print(f"  {zone:<14}{event_by_zone[zone]:>12}{processing_by_zone[zone]:>17}")
    print(f"  {'TOTAL':<14}{sum(event_by_zone.values()):>12}{sum(processing_by_zone.values()):>17}")

    late_by_zone = Counter(e["zone"] for e in late_events)
    zones = ", ".join(f"{z} {n}" for z, n in late_by_zone.most_common()) or "-"
    print(f"late events -> side output: {len(late_events)}  ({zones})")
    for e in late_events[:MAX_LATE_EXAMPLES]:
        print(f"  {e['zone']:<14} event_time {fmt_ts(parse_ts(e['event_time']))}  "
              f"arrived {fmt_ts(parse_ts(e['processing_time']))}  "
              f"lateness {e['lateness_seconds']:>9.0f}s")
    if len(late_events) > MAX_LATE_EXAMPLES:
        print(f"  ... and {len(late_events) - MAX_LATE_EXAMPLES} more")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--through-day", type=int, metavar="N",
                        help="stop after simulated day N (by processing_time); rerun to resume")
    parser.add_argument("--reset", action="store_true", help="delete the checkpoint and start from the beginning")
    args = parser.parse_args()

    if args.reset and CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()
        print(f"Deleted checkpoint {CHECKPOINT_PATH}")

    state = load_state()
    stop_at = START_DATE + timedelta(days=args.through_day) if args.through_day else None

    print(f"Window {WINDOW_SIZE}, watermark delay {WATERMARK_DELAY}, allowed lateness {ALLOWED_LATENESS}")
    if state.offset:
        print(f"Resuming from line {state.offset + 1} (batch {state.batch_number + 1})")
    if state.pending_end_offset is not None:
        print(f"Previous run stopped while writing batch {state.batch_number + 1}; "
              f"replaying lines {state.offset + 1}-{state.pending_end_offset}")
    if stop_at:
        print(f"Stopping before processing_time {fmt_ts(stop_at)} (end of day {args.through_day})")

    batches_run = 0
    for events in read_batches(state.offset, stop_at, state.pending_end_offset):
        # 1. Record which lines this batch covers before writing anything, so a
        #    crash during the writes can be replayed with the same boundaries.
        state.pending_end_offset = state.offset + len(events)
        save_state(state)

        # 2. Process the batch. This is deterministic: the same starting state
        #    and the same lines always produce the same results.
        state.offset += len(events)
        state.batch_number += 1
        closed_event, closed_processing, late_events = process_batch(state, events)
        print_batch_summary(state, events, closed_event, closed_processing, late_events)

        # 3. Write the tables. Tables that already have this batch skip it.
        write_batch(state.batch_number, events, closed_event, closed_processing, late_events)

        # 4. Mark the batch complete.
        state.pending_end_offset = None
        save_state(state)
        batches_run += 1

    print(f"\nDone: {batches_run} batches this run, {state.offset} lines processed in total, "
          f"{state.total_late} late events in total.")
    print(f"Still open: {len(state.event_windows)} event-time windows, "
          f"{len(state.processing_windows)} processing-time windows.")


if __name__ == "__main__":
    main()
