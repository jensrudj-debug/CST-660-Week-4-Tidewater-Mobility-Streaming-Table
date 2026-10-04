"""Health-signal queries over the Delta tables in lake/.

Prints three tables:
  1. Event-time vs processing-time trip counts for a few busy windows
  2. Side-output rate per zone (late events / all events)
  3. Window completeness per zone (on-time / (on-time + late))

Run after the consumer has processed the stream:

    python demo_queries.py
"""

import pandas as pd
from deltalake import DeltaTable

from bad_correction import CORRECTION_JOB
from consumer import EVENT_COUNTS_TABLE, LATE_EVENTS_TABLE, PROCESSING_COUNTS_TABLE, TRIPS_RAW_TABLE

HARBOR_ZONE = "harbor_tunnel"
OTHER_ZONE = "downtown"
BUSY_WINDOWS = 8
COMPLETENESS_THRESHOLD = 0.97
WORST_WINDOWS = 8
WIDTH = 64


def banner(title: str) -> None:
    print("\n" + "=" * WIDTH)
    print(f"  {title}")
    print("=" * WIDTH)


def load(table) -> pd.DataFrame:
    return DeltaTable(str(table)).to_pandas()


def side_by_side(event: pd.DataFrame, processing: pd.DataFrame) -> None:
    banner("1. EVENT-TIME vs PROCESSING-TIME trips per 15-min window")

    def counts(df, zone):
        return df[df.zone == zone].set_index("window_start").trip_count

    cols = {}
    for zone in (HARBOR_ZONE, OTHER_ZONE):
        e, p = counts(event, zone), counts(processing, zone)
        both = pd.concat([e.rename("event"), p.rename("proc")], axis=1, sort=True).fillna(0).astype(int)
        both["diff"] = both.proc - both.event
        cols[zone] = both

    # Busiest windows by event-time trips across both zones, shown in time order
    busiest = (cols[HARBOR_ZONE].event + cols[OTHER_ZONE].event).nlargest(BUSY_WINDOWS).index.sort_values()

    print(f"{'':<12}{HARBOR_ZONE:^24}{OTHER_ZONE:^24}")
    print(f"{'window':<12}" + f"{'event':>8}{'proc':>8}{'diff':>8}" * 2)
    print("-" * WIDTH)
    for start in busiest:
        row = f"{start:%m-%d %H:%M} "
        for zone in (HARBOR_ZONE, OTHER_ZONE):
            r = cols[zone].loc[start]
            row += f"{r.event:>8}{r.proc:>8}{r['diff']:>+8}"
        print(row)
    print("-" * WIDTH)
    print("diff = processing-time count - event-time count")
    for zone in (HARBOR_ZONE, OTHER_ZONE):
        mismatched = (cols[zone]["diff"] != 0).mean()
        print(f"  {zone:<14} windows where the two counts disagree: {mismatched:>5.0%}")


def side_output_rate(trips: pd.DataFrame, late: pd.DataFrame) -> None:
    banner("2. SIDE-OUTPUT RATE: late events / all events")
    rates = pd.DataFrame({
        "events": trips.groupby("zone").size(),
        "late": late.groupby("zone").size(),
    }).fillna(0).astype(int)
    rates.loc["ALL ZONES"] = rates.sum()
    rates["rate"] = rates.late / rates.events

    print(f"{'zone':<16}{'events':>10}{'late':>10}{'rate':>10}")
    print("-" * 46)
    for r in rates.itertuples():
        if r.Index == "ALL ZONES":
            print("-" * 46)
        print(f"{r.Index:<16}{r.events:>10}{r.late:>10}{r.rate:>10.1%}")


def completeness(event: pd.DataFrame, late: pd.DataFrame) -> None:
    banner("3. WINDOW COMPLETENESS: on-time / (on-time + late)")
    on_time = event.groupby(["zone", "window_start"]).trip_count.sum().rename("on_time")
    late_n = late.groupby(["zone", "window_start"]).size().rename("late")
    windows = pd.concat([on_time, late_n], axis=1).fillna(0).astype(int)
    windows["completeness"] = windows.on_time / (windows.on_time + windows.late)
    windows["flagged"] = windows.completeness < COMPLETENESS_THRESHOLD

    per_zone = windows.groupby(level="zone").agg(
        windows=("on_time", "size"),
        on_time=("on_time", "sum"),
        late=("late", "sum"),
        flagged=("flagged", "sum"),
    )
    per_zone["overall"] = per_zone.on_time / (per_zone.on_time + per_zone.late)

    print(f"{'zone':<16}{'windows':>9}{'overall':>10}{f'< {COMPLETENESS_THRESHOLD:.0%}':>10}{'flagged %':>11}")
    print("-" * 56)
    for r in per_zone.itertuples():
        print(f"{r.Index:<16}{r.windows:>9}{r.overall:>10.1%}{r.flagged:>10}{r.flagged / r.windows:>11.0%}")

    print(f"\nWorst {WORST_WINDOWS} windows (flagged below {COMPLETENESS_THRESHOLD:.0%}):")
    print(f"{'zone':<16}{'window':<14}{'on-time':>9}{'late':>7}{'complete':>10}")
    print("-" * 56)
    worst = windows[windows.flagged].sort_values(["completeness", "late"], ascending=[True, False])
    for r in worst.head(WORST_WINDOWS).itertuples():
        zone, start = r.Index
        print(f"{zone:<16}{start:%m-%d %H:%M}   {r.on_time:>9}{r.late:>7}{r.completeness:>10.0%}  FLAG")


def main() -> None:
    latest = DeltaTable(str(EVENT_COUNTS_TABLE)).history(1)[0]
    if latest.get("job") == CORRECTION_JOB:
        print(f"WARNING: zone_counts_event_time is at the bad correction (v{latest['version']}). "
              f"Run demo_time_travel.py to restore it first.")

    event = load(EVENT_COUNTS_TABLE)
    processing = load(PROCESSING_COUNTS_TABLE)
    trips = load(TRIPS_RAW_TABLE)
    late = load(LATE_EVENTS_TABLE)

    side_by_side(event, processing)
    side_output_rate(trips, late)
    completeness(event, late)


if __name__ == "__main__":
    main()
