"""Schema evolution demo on lake/trips_raw.

Run after the consumer has processed the stream through day 3:

    python consumer.py --through-day 3
    python demo_schema.py

Or let this script rebuild the lake from scratch through day 3 first:

    python demo_schema.py --fresh
"""

import argparse
import shutil
import subprocess
import sys

import pyarrow as pa
from deltalake import DeltaTable, QueryBuilder

from consumer import LAKE_DIR, TRIPS_RAW_TABLE

TABLE_URI = str(TRIPS_RAW_TABLE)
NEW_COLUMN = "surge_multiplier"
SAMPLE_ROWS = 4


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def run_consumer(through_day: int) -> None:
    print(f"$ python consumer.py --through-day {through_day}")
    result = subprocess.run(
        [sys.executable, "consumer.py", "--through-day", str(through_day)],
        capture_output=True, text=True, check=True,
    )
    # Only show the consumer's final summary to keep the demo readable
    for line in result.stdout.strip().splitlines()[-2:]:
        print(f"  {line}")


def print_schema(dt: DeltaTable) -> None:
    print(f"Table: {TABLE_URI}   version: {dt.version()}")
    print(f"  {'column':<18} type")
    print(f"  {'-' * 18} {'-' * 10}")
    for field in dt.schema().fields:
        marker = "   <-- NEW" if field.name == NEW_COLUMN else ""
        print(f"  {field.name:<18} {field.type.type}{marker}")


def query(dt: DeltaTable, sql: str) -> None:
    print(f"SQL> {sql}\n")
    # QueryBuilder returns an Arrow stream (arro3); pyarrow can read it directly
    result = QueryBuilder().register("trips_raw", dt).execute(sql)
    df = pa.table(result.read_all()).to_pandas()
    print(df.to_string(index=False, na_rep="NULL"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fresh", action="store_true",
                        help="delete lake/ and run the consumer through day 3 before the demo")
    args = parser.parse_args()

    if args.fresh:
        banner("SETUP: rebuild the lake through day 3")
        shutil.rmtree(LAKE_DIR, ignore_errors=True)
        run_consumer(3)

    if not DeltaTable.is_deltatable(TABLE_URI):
        sys.exit(f"{TABLE_URI} does not exist. Run: python demo_schema.py --fresh")
    dt = DeltaTable(TABLE_URI)
    if NEW_COLUMN in dt.schema().to_arrow().names:
        sys.exit(f"{TABLE_URI} already has {NEW_COLUMN}; the stream is past day 3. "
                 f"Run: python demo_schema.py --fresh")

    # ------------------------------------------------------------------
    banner("STEP 1: trips_raw schema after days 1-3 (before the schema change)")
    print_schema(dt)
    files_before = set(dt.file_uris())
    version_before = dt.version()
    print(f"\nData files: {len(files_before)}   rows: {dt.to_pyarrow_table().num_rows}")

    # ------------------------------------------------------------------
    banner("STEP 2: stream day 4, when phones start sending surge_multiplier")
    run_consumer(4)
    dt = DeltaTable(TABLE_URI)
    print()
    print_schema(dt)

    files_after = set(dt.file_uris())
    print(f"\nData files: {len(files_after)}   rows: {dt.to_pyarrow_table().num_rows}")
    print(f"Old data files still in the table unchanged: "
          f"{len(files_before & files_after)} of {len(files_before)}")
    print(f"New data files added for day 4:              {len(files_after - files_before)}")

    print(f"\nCommits since version {version_before}:")
    for commit in sorted(dt.history(dt.version() - version_before), key=lambda c: c["version"]):
        metrics = commit["operationMetrics"]
        print(f"  v{commit['version']:<3} {commit['operation']} {commit['operationParameters']['mode']:<7}"
              f" +{metrics['num_added_files']} files, -{metrics['num_removed_files']} files,"
              f" +{metrics['num_added_rows']} rows")
    print("No files removed: the new column was added to the table metadata only.")

    # ------------------------------------------------------------------
    banner("STEP 3: old rows read surge_multiplier as NULL, with no rewrite")
    print("Day 1 trips (written before the column existed):\n")
    query(dt, f"""SELECT trip_id, zone, event_time, fare, {NEW_COLUMN}, batch_id
FROM trips_raw WHERE event_time < '2026-09-02' ORDER BY event_time LIMIT {SAMPLE_ROWS}""")
    print("\nDay 4 trips (written after the column appeared):\n")
    query(dt, f"""SELECT trip_id, zone, event_time, fare, {NEW_COLUMN}, batch_id
FROM trips_raw WHERE event_time >= '2026-09-04' ORDER BY event_time LIMIT {SAMPLE_ROWS}""")

    print("\nsurge_multiplier coverage by event day:\n")
    query(dt, f"""SELECT CAST(event_time AS DATE) AS event_day,
       COUNT(*) AS trips,
       COUNT({NEW_COLUMN}) AS with_surge,
       COUNT(*) - COUNT({NEW_COLUMN}) AS surge_null
FROM trips_raw GROUP BY 1 ORDER BY 1""")


if __name__ == "__main__":
    main()
