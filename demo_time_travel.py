"""Time-travel demo: prove what zone_counts_event_time looked like before the
bad late-data correction, find the offending commit, and restore the table.

Run after bad_correction.py:

    python bad_correction.py
    python demo_time_travel.py
"""

import json
import sys
from datetime import timedelta

import pandas as pd
from deltalake import DeltaTable

from bad_correction import CORRECTION_JOB, RECORD_PATH

ZONE = "harbor_tunnel"
HISTORY_COMMITS = 6


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def day_counts(dt: DeltaTable, day_start, zone=None) -> pd.DataFrame:
    df = dt.to_pandas()
    df = df[(df.window_start >= day_start) & (df.window_start < day_start + timedelta(days=1))]
    if zone:
        df = df[df.zone == zone]
    return df


def hourly(df: pd.DataFrame) -> pd.Series:
    """Roll 15-minute windows up to hours so the day fits on one screen."""
    return df.groupby(df.window_start.dt.hour).trip_count.sum()


def sorted_rows(dt: DeltaTable) -> pd.DataFrame:
    df = dt.to_pandas()
    return df.sort_values(["window_start", "zone"]).reset_index(drop=True)


def main() -> None:
    if not RECORD_PATH.exists():
        sys.exit(f"{RECORD_PATH} not found. Run: python bad_correction.py")
    record = json.loads(RECORD_PATH.read_text())
    table_uri = record["table"]
    good_version, bad_version = record["good_version"], record["bad_version"]
    day_start = pd.Timestamp(record["day_start"])

    latest = DeltaTable(table_uri)
    if latest.version() != bad_version:
        sys.exit(f"{table_uri} is at v{latest.version()}, not the bad correction v{bad_version}. "
                 f"Run: python bad_correction.py")

    # ------------------------------------------------------------------
    banner(f"STEP 1: day {record['day']} {ZONE} counts, good v{good_version} vs latest v{bad_version}")
    good = DeltaTable(table_uri, version=good_version)  # time travel
    good_day = day_counts(good, day_start, ZONE)
    bad_day = day_counts(latest, day_start, ZONE)

    print(f"{ZONE} trips per hour on {day_start:%Y-%m-%d} (sum of four 15-minute windows)\n")
    side_by_side = pd.DataFrame({
        f"v{good_version} (before)": hourly(good_day),
        f"v{bad_version} (latest)": hourly(bad_day),
    }).reindex(range(24), fill_value=0).fillna(0).astype(int)
    side_by_side["missing"] = side_by_side.iloc[:, 0] - side_by_side.iloc[:, 1]
    side_by_side.index = [f"{h:02d}:00" for h in side_by_side.index]
    side_by_side.loc["TOTAL"] = side_by_side.sum()
    print(side_by_side.to_string())
    print(f"\nnon-empty 15-minute windows: {len(good_day)} before, {len(bad_day)} latest")

    # ------------------------------------------------------------------
    banner("STEP 2: table history, find the offending commit")
    print(f"  {'ver':<5}{'committed (UTC)':<21}{'operation':<10}{'mode':<11}"
          f"{'files +':>8}{'files -':>8}{'rows +':>8}  details")
    for commit in latest.history(HISTORY_COMMITS):
        params = commit.get("operationParameters", {})
        metrics = commit.get("operationMetrics", {})
        committed = pd.Timestamp(commit["timestamp"], unit="ms").strftime("%Y-%m-%d %H:%M:%S")
        details = ""
        if commit.get("job") == CORRECTION_JOB:
            details = f"job={commit['job']}: {commit['description']}   <-- BAD CORRECTION"
        elif commit["operation"] == "RESTORE":
            details = f"restored to v{params['version']}"
        print(f"  v{commit['version']:<4}{committed:<21}{commit['operation']:<10}{params.get('mode', ''):<11}"
              f"{metrics.get('num_added_files', '')!s:>8}{metrics.get('num_removed_files', '')!s:>8}"
              f"{metrics.get('num_added_rows', '')!s:>8}  {details}")
    print(f"\nv{bad_version} replaced day {record['day']}'s files with one new file of "
          f"{len(day_counts(latest, day_start))} windows rebuilt from late_events.")

    # ------------------------------------------------------------------
    banner(f"STEP 3: restore the table to v{good_version}")
    result = latest.restore(good_version)
    restored = DeltaTable(table_uri)
    print(f"restore metrics: {result}")
    print(f"table is now at v{restored.version()} (a new commit; v{bad_version} is still in history)")

    restored_day = day_counts(restored, day_start, ZONE)
    print(f"\n{ZONE} day {record['day']} total: before {good_day.trip_count.sum()}, "
          f"bad {bad_day.trip_count.sum()}, restored {restored_day.trip_count.sum()}")
    same = sorted_rows(restored).equals(sorted_rows(good))
    print(f"Whole table identical to v{good_version}: {same}")


if __name__ == "__main__":
    main()
