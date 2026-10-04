# Tidewater Mobility: Event-Time Zone Counts Prototype

A working prototype of a late-data-safe pipeline for surge-pricing zone counts:
a synthetic trip event stream, a micro-batch consumer that windows by event time
with an explicit watermark and a late-event side output, and Delta Lake tables
with schema evolution and time travel.

## The business problem

Surge pricing depends on accurate trips-per-zone counts in short time windows.
Two things have been going wrong:

1. **Harbor zones are undercounted.** Drivers' phones lose connectivity in the
   harbor tunnels and buffer trip events for minutes at a time. When counts are
   bucketed by *when an event arrives* (processing time) instead of *when the trip
   happened* (event time), tunnel trips land in the wrong window: the window they
   belong to comes up short, and a later window is inflated when the phone
   reconnects and flushes its buffer.
2. **A correction job destroyed a day of aggregates.** Last week a late-data
   correction overwrote a full day of zone aggregates with bad numbers, and there
   was no easy way to prove what the table had looked like before or to roll it
   back.

This prototype shows how to handle both: count by event time, decide explicitly
how long to wait for late data, keep (not drop) anything later than that, and
store results in a table format with a versioned history.

## What's in the repo

| File | Purpose |
|---|---|
| `generator.py` | Writes 6 days of synthetic trip events to `events.jsonl`, ordered by arrival, with tunnel delays, very late and next-morning arrivals, and a schema change on day 4 |
| `consumer.py` | Micro-batch consumer: tails `events.jsonl` 500 lines at a time, computes 15-minute event-time and processing-time windows per zone, applies the watermark, routes late events to a side output, and appends everything to Delta tables |
| `demo_schema.py` | Shows `trips_raw` before and after `surge_multiplier` appears, and that old rows read it as NULL without a rewrite |
| `demo_queries.py` | Health signals: event-time vs processing-time counts, side-output rate per zone, window completeness |
| `bad_correction.py` | Replays the incident: overwrites day 2 of the zone counts with numbers rebuilt only from late events |
| `demo_time_travel.py` | Reads the table as it was before the correction, finds the offending commit in the history, and restores the table |
| `run_demo.sh` | Rebuilds everything from scratch and runs all of the above in order |

## Install

Requires Python 3.10 or newer (developed on 3.14) and bash for `run_demo.sh`
(Git Bash on Windows).

```bash
python -m venv .venv

# Windows (PowerShell)
.\.venv\Scripts\Activate.ps1
# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

Pinned versions: `deltalake==1.6.6`, `pandas==3.0.6`, `pyarrow==25.0.1`.

## Running it

Everything at once, from a clean slate (about 2-4 minutes):

```bash
./run_demo.sh            # straight through
PAUSE=1 ./run_demo.sh    # wait for Enter before each step
```

Or step by step, with the virtual environment active:

```bash
python generator.py                     # 1. generate events.jsonl (120,000 events)
rm -rf lake                             # 2. start clean
python consumer.py --through-day 3      # 3. stream days 1-3
python demo_schema.py                   # 4. schema evolution (streams day 4 itself)
python consumer.py --through-day 6      # 5. stream days 5-6
python consumer.py --late-examples 20   # 6. drain the next-morning arrivals
python demo_queries.py                  # 7. health signals
python bad_correction.py                # 8. replay the bad correction
python demo_time_travel.py              # 9. time travel and restore
```

In Windows PowerShell, use `Remove-Item -Recurse -Force lake` in place of
`rm -rf lake`.

### Consumer options

| Option | Effect |
|---|---|
| `--through-day N` | Stop at the end of simulated day N (by processing time). Running again resumes from the checkpoint. |
| `--reset` | Delete the checkpoint and start from line 1. Delete `lake/` as well for a truly fresh run. |
| `--late-examples N` | Print up to N late events per batch (default 5). |

## The event stream

`generator.py` produces 20,000 trips per day for 6 days (2026-09-01 to
2026-09-06, UTC) across six zones: `downtown`, `airport`, `university`,
`waterfront`, `harbor_north`, `harbor_tunnel`. The seed is fixed, so every run
produces the same file.

Each event has `trip_id`, `zone`, `event_time` (when the trip happened),
`processing_time` (when it reached the pipeline) and `fare`. From day 4 on,
events also carry `surge_multiplier`; events before day 4 do not have the key at
all. The file is sorted by `processing_time`, so event times are out of order,
just as they would be on a real stream.

| Delay group | Share | Delay | Where |
|---|---|---|---|
| normal | ~84.5% | 1-5 seconds | all zones |
| tunnel | ~12% | 2-8 minutes | ~29% of harbor trips, ~3.6% elsewhere |
| very late | ~3% | 20-60 minutes | all zones |
| next morning | ~0.5% | arrives 06:00-09:00 the following day | all zones |

All zones have roughly equal volume (about 20,000 trips each). The harbor zones
differ only in how often their trips are delayed by the tunnel.

## Windows and watermark

| Setting | Value | Constant in `consumer.py` |
|---|---|---|
| Window | 15-minute tumbling, by `event_time` | `WINDOW_SIZE` |
| Watermark delay | 2 minutes | `WATERMARK_DELAY` |
| Allowed lateness | 10 minutes | `ALLOWED_LATENESS` |
| Micro-batch size | 500 lines | `BATCH_SIZE` |

**How it works**

```
watermark = latest event_time seen so far − 2 minutes
a window closes when   window_end + 10 minutes ≤ watermark
```

When a window closes, its count is final: it is written to
`zone_counts_event_time` and removed from memory. Any event that arrives for a
window that has already closed is written to `late_events` together with the
watermark at the time it arrived and how far behind it was. Nothing is
dropped. The watermark only moves forward, so a late event with an old timestamp
never drags it back.

In practice, a trip is counted in its correct window as long as it arrives before
the pipeline has seen a trip from 12 minutes after the end of that window.

**Why these values**

- **2-minute watermark delay** covers ordinary network jitter and reordering
  between phones (seconds), with plenty of margin.
- **10 minutes of allowed lateness** is sized to the harbor tunnel. Tunnel
  buffering runs 2-8 minutes, so with a 12-minute total every tunnel-delayed trip
  is counted in the window it actually happened in. That fixes the harbor
  undercount at its source.
- **Not longer.** Every extra minute of waiting delays the final count that
  pricing acts on. Delays of 20+ minutes are a different failure (a phone off the
  network for a long time, an app stuck in the background) and waiting an hour
  for them would make surge pricing an hour stale. Those events go to the side
  output, where they can be reconciled separately.

The watermark is advanced after every event rather than once per batch, so the
lateness rule behaves the same regardless of how many lines are in a batch.
Closed windows are still written at the end of each batch.

**Why some windows stay open at the end.** After the full stream, the last
15-minute window of day 6 (23:45-00:00) is still open in every zone. Closing it
would need a trip from 00:12 on day 7, and the stream stops before then.
On a live stream the next day's trips would close it.

### Idempotent, resumable writes

The consumer checkpoints its state (file offset, watermark, open windows) to
`lake/_checkpoints/consumer_state.json` after every batch, so it can be
stopped and resumed.

Each Delta append is tagged with an application transaction
(`app_id = "tidewater-consumer"`, `version = batch_id`) that is committed
atomically with the data. Before appending, the consumer asks each table which
batch it last committed and skips the write if the batch is already there. The
line range of each batch is recorded before any table is written, so if the
process crashes mid-batch, the next run replays exactly the same lines. A crash
at any point therefore produces no duplicates and no gaps.

## Delta tables

All tables live under `lake/`. Every row carries the `batch_id` of the
micro-batch that wrote it. Appends use `schema_mode="merge"`, so new columns are
added to the table schema instead of failing the write.

| Table | One row per | Columns |
|---|---|---|
| `trips_raw` | event, as received | `trip_id`, `zone`, `event_time`, `processing_time`, `fare`, `batch_id`, `surge_multiplier` (from day 4) |
| `zone_counts_event_time` | closed event-time window per zone | `zone`, `window_start`, `trip_count`, `batch_id` |
| `zone_counts_processing_time` | closed processing-time window per zone (the naive count) | `zone`, `window_start`, `trip_count`, `batch_id` |
| `late_events` | event that arrived after its window closed (the side output) | all `trips_raw` columns plus `window_start`, `window_end`, `watermark`, `lateness_seconds` |

`zone_counts_event_time` holds only events that arrived on time;
`zone_counts_event_time` plus `late_events` accounts for every event. After the
full run: 115,763 on time + 4,034 late + 203 in the open final windows = 120,000.

## Schema evolution demo

```bash
python consumer.py --through-day 3
python demo_schema.py
```

Or `python demo_schema.py --fresh` to rebuild `lake/` through day 3 first.

1. Prints the `trips_raw` schema after days 1-3: no `surge_multiplier`.
2. Streams day 4, then prints the schema again with `surge_multiplier` added.
   All 120 data files from days 1-3 are still in the table untouched; the 40
   commits for day 4 are all appends that removed no files.
3. Queries day 1 rows (`surge_multiplier` is NULL) and day 4 rows (values such
   as 1.25, 1.5), plus a per-day count showing days 1-3 are entirely NULL and
   day 4 is fully populated.

Old rows were never rewritten: the new column exists only in the table
metadata, and readers fill it with NULL for files written before it existed.

## Time-travel demo

Run after the full stream has been processed:

```bash
python bad_correction.py
python demo_time_travel.py
```

`bad_correction.py` reproduces last week's mistake. It rebuilds day 2 of
`zone_counts_event_time` from `late_events` alone and overwrites only day 2
(`mode="overwrite"` with a `window_start` predicate). The commit is tagged
`job=late_data_correction`. Day 2 drops from 19,318 trips to 682; the other
days are untouched. The versions before and after are saved in
`lake/_checkpoints/bad_correction.json`.

`demo_time_travel.py` then:

1. Loads the table **as of the version before the correction** and the latest
   version, and prints `harbor_tunnel`'s day 2 counts side by side per hour
   (3,212 trips before, 98 after).
2. Prints the table history. The bad commit stands out: an `Overwrite` that
   removed 41 files, labelled `late_data_correction`, after a long run of
   appends.
3. Restores the table to the good version. The restore is itself a new
   commit, so the bad version stays in the history for audit. The script then
   confirms the whole table is identical to the pre-correction version.

The pair can be run repeatedly; each round adds commits and ends restored.

## Reading the health signals

`demo_queries.py` prints three tables.

**1. Event-time vs processing-time counts.** For the busiest windows in
`harbor_tunnel` and `downtown`, the two counting methods disagree by a few trips
per window, and over the whole period they disagree in 86% of `harbor_tunnel`
windows against 76% of `downtown` windows. Processing-time counts move trips into
whichever window they *arrived* in; harbor trips are delayed far more often, so
their windows are shuffled more.

**2. Side-output rate** (late events / all events per zone). It is 3.3-3.5% in
every zone, 3.4% overall. The rate is flat across zones because every tunnel
delay fits inside the 12-minute allowance; what remains is the uniform tail of
20+ minute and next-morning arrivals. A rise in any one zone means its delays
have moved past the watermark.

**3. Window completeness** (on-time / (on-time + late) per window). Overall it
is 96.5-96.7% in every zone, and windows below 97% are flagged; the worst are
around 82-85%. Completeness says how much of a window's true count was
available when pricing used it.

### Symptom versus cause

| What the pricing team sees (symptom) | What is actually happening (cause) |
|---|---|
| Harbor surge doesn't trigger when it should, then overshoots a few minutes later | Phones buffer trips in the tunnel; processing-time counts put those trips in the window they arrived in, not the window they happened in |
| One zone's counts start running consistently low | That zone's delays have grown past the 12-minute allowance (for example a longer tunnel outage); its side-output rate and completeness both move |
| A whole day of zone aggregates suddenly collapses | A correction job overwrote on-time counts with a partial rebuild; visible as an `Overwrite` in the table history |

The side-output rate and window completeness catch the second row before the
pricing team does. The table history and time travel resolve the third.

## Resetting

```bash
rm -rf lake            # delete all tables and the consumer checkpoint
python generator.py    # regenerate events.jsonl (deterministic)
```

`events.jsonl` and `lake/` are not tracked in git.
