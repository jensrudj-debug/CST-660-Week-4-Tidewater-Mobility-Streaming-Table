"""Synthetic rideshare trip event generator for Tidewater Mobility.

Writes events.jsonl in processing-time order, so event_time arrives out of
order and some events arrive very late, a few not until the next morning. From simulated day 4 onward each
event carries a new surge_multiplier field (schema change).
"""

import json
import random
import uuid
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SEED = 42
NUM_DAYS = 6
TRIPS_PER_DAY = 2000
START_DATE = datetime(2026, 9, 1, tzinfo=timezone.utc)
OUTPUT_PATH = "events.jsonl"

ZONES = ["downtown", "airport", "university", "waterfront", "harbor_north", "harbor_tunnel"]
HARBOR_ZONES = ["harbor_north", "harbor_tunnel"]

# Delay mix: group -> (share of events, min delay seconds, max delay seconds)
# next_morning ignores the min/max delay and uses NEXT_MORNING_HOURS instead.
DELAY_MIX = {
    "normal": (0.845, 1, 5),
    "tunnel": (0.12, 2 * 60, 8 * 60),
    "very_late": (0.03, 20 * 60, 60 * 60),
    "next_morning": (0.005, None, None),
}
# next_morning events arrive between these UTC hours on the day after they occur
NEXT_MORNING_HOURS = (6, 9)
# Share of tunnel-delayed events that originate in a harbor zone
TUNNEL_HARBOR_SHARE = 0.80

# First simulated day (1-based) on which surge_multiplier appears
SURGE_START_DAY = 4
SURGE_VALUES = [1.0, 1.25, 1.5, 1.75, 2.0]
SURGE_WEIGHTS = [0.50, 0.20, 0.15, 0.10, 0.05]


def iso(ts: datetime) -> str:
    return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def pick_zone(group: str, rng: random.Random) -> str:
    if group == "tunnel":
        if rng.random() < TUNNEL_HARBOR_SHARE:
            return rng.choice(HARBOR_ZONES)
        return rng.choice([z for z in ZONES if z not in HARBOR_ZONES])
    return rng.choice(ZONES)


def generate(rng: random.Random) -> list[tuple[str, dict]]:
    """Return (delay_group, event) pairs. delay_group is kept out of the event."""
    groups = list(DELAY_MIX)
    weights = [DELAY_MIX[g][0] for g in groups]
    events = []

    for day in range(1, NUM_DAYS + 1):
        day_start = START_DATE + timedelta(days=day - 1)
        for _ in range(TRIPS_PER_DAY):
            group = rng.choices(groups, weights=weights)[0]
            _, lo, hi = DELAY_MIX[group]

            event_time = day_start + timedelta(seconds=rng.uniform(0, 86400))
            if group == "next_morning":
                first, last = NEXT_MORNING_HOURS
                next_morning = day_start + timedelta(days=1, hours=first)
                processing_time = next_morning + timedelta(seconds=rng.uniform(0, (last - first) * 3600))
            else:
                processing_time = event_time + timedelta(seconds=rng.uniform(lo, hi))

            event = {
                "trip_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                "zone": pick_zone(group, rng),
                "event_time": iso(event_time),
                "processing_time": iso(processing_time),
                "fare": round(rng.uniform(6.0, 65.0), 2),
            }
            if day >= SURGE_START_DAY:
                event["surge_multiplier"] = rng.choices(SURGE_VALUES, weights=SURGE_WEIGHTS)[0]

            events.append((group, event))

    events.sort(key=lambda ge: ge[1]["processing_time"])
    return events


def main() -> None:
    rng = random.Random(SEED)
    events = generate(rng)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for _, event in events:
            f.write(json.dumps(event) + "\n")

    counts = {g: 0 for g in DELAY_MIX}
    examples = {}
    for group, event in events:
        counts[group] += 1
        if group == "tunnel" and event["zone"] not in HARBOR_ZONES:
            continue  # prefer a harbor-zone example for the tunnel group
        examples.setdefault(group, event)

    total = len(events)
    print(f"Wrote {total} events to {OUTPUT_PATH}\n")
    print("Delay group counts:")
    for group, n in counts.items():
        print(f"  {group:<12} {n:>6}  ({n / total:.1%})")
    print()
    for group in DELAY_MIX:
        print(f"Example {group} event:")
        print("  " + json.dumps(examples[group]))


if __name__ == "__main__":
    main()
