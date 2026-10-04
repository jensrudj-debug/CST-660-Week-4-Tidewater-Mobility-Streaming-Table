"""Simulate last week's bad late-data correction job.

Overwrites simulated day 2 of lake/zone_counts_event_time with counts rebuilt
only from late_events. That throws away every on-time trip for the day, so
the "corrected" counts are far too low. Other days are left untouched.

The version before and after is recorded in lake/_checkpoints/ so
demo_time_travel.py can find the good version again.

Run after the consumer has processed the full stream:

    python bad_correction.py
"""

import json
import sys
from datetime import timedelta

import pyarrow as pa
from deltalake import CommitProperties, DeltaTable, write_deltalake

from consumer import CHECKPOINT_PATH, EVENT_COUNTS_TABLE, LATE_EVENTS_TABLE, WINDOW_SCHEMA
from generator import START_DATE

BAD_DAY = 2
CORRECTION_JOB = "late_data_correction"
RECORD_PATH = CHECKPOINT_PATH.parent / "bad_correction.json"


def main() -> None:
    table_uri = str(EVENT_COUNTS_TABLE)
    dt = DeltaTable(table_uri)

    latest = dt.history(1)[0]
    if latest.get("job") == CORRECTION_JOB:
        sys.exit(f"The latest commit (v{latest['version']}) is already the bad correction. "
                 f"Run demo_time_travel.py to restore the table first.")

    day_start = START_DATE + timedelta(days=BAD_DAY - 1)
    day_end = day_start + timedelta(days=1)
    version_before = dt.version()
    print(f"zone_counts_event_time version BEFORE correction: {version_before}")

    # The mistake: rebuild day 2 from the side output alone. late_events only
    # holds trips that missed their window, so every on-time trip is lost.
    late = DeltaTable(str(LATE_EVENTS_TABLE)).to_pandas()
    day_late = late[(late.window_start >= day_start) & (late.window_start < day_end)]
    rebuilt = (day_late.groupby(["zone", "window_start"]).size()
               .rename("trip_count").reset_index())
    rebuilt["batch_id"] = None  # not produced by the streaming consumer
    data = pa.Table.from_pandas(rebuilt, schema=WINDOW_SCHEMA, preserve_index=False)

    current = dt.to_pandas()
    current_day = current[(current.window_start >= day_start) & (current.window_start < day_end)]
    print(f"Day {BAD_DAY} ({day_start:%Y-%m-%d}) currently: {len(current_day)} windows, "
          f"{current_day.trip_count.sum()} trips")
    print(f"Replacing with counts rebuilt from late_events only: {len(rebuilt)} windows, "
          f"{rebuilt.trip_count.sum()} trips")

    # overwrite + predicate replaces only the rows matching the predicate.
    # Rows for every other day stay exactly as they were.
    predicate = (f"window_start >= '{day_start.isoformat()}' "
                 f"AND window_start < '{day_end.isoformat()}'")
    write_deltalake(
        table_uri, data, mode="overwrite", predicate=predicate,
        commit_properties=CommitProperties(custom_metadata={
            "job": CORRECTION_JOB,
            "description": f"rebuild day {BAD_DAY} ({day_start:%Y-%m-%d}) window counts from late_events",
        }),
    )

    dt = DeltaTable(table_uri)
    version_after = dt.version()
    print(f"zone_counts_event_time version AFTER correction:  {version_after}")
    print(f"Overwrite predicate: {predicate}")

    RECORD_PATH.write_text(json.dumps({
        "table": table_uri,
        "good_version": version_before,
        "bad_version": version_after,
        "day": BAD_DAY,
        "day_start": day_start.isoformat(),
    }, indent=2))
    print(f"Recorded versions in {RECORD_PATH}")

    after = dt.to_pandas()
    after_day = after[(after.window_start >= day_start) & (after.window_start < day_end)]
    other_before = current[~current.index.isin(current_day.index)]
    other_after = after[(after.window_start < day_start) | (after.window_start >= day_end)]
    print(f"\nDay {BAD_DAY} now: {len(after_day)} windows, {after_day.trip_count.sum()} trips "
          f"(was {current_day.trip_count.sum()})")
    print(f"Other days: {other_after.trip_count.sum()} trips (was {other_before.trip_count.sum()}), untouched")


if __name__ == "__main__":
    main()
