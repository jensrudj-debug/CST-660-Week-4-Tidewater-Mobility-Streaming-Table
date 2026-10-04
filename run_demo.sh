#!/usr/bin/env bash
# Rebuild everything from scratch and run the full demo in order.
#
#   ./run_demo.sh            run straight through
#   PAUSE=1 ./run_demo.sh    wait for Enter before each step
#
# Consumer runs print one summary per micro-batch (hundreds of batches), so
# this script shows only the last batch and the run summary for each of them.
last_batch() {
    awk '/^=== Batch/ { buf = "" } { buf = buf $0 "\n" } END { printf "%s", buf }'
}

set -euo pipefail
cd "$(dirname "$0")"

if [ -x .venv/Scripts/python.exe ]; then
    PY=.venv/Scripts/python.exe   # Windows venv
elif [ -x .venv/bin/python ]; then
    PY=.venv/bin/python           # macOS / Linux venv
else
    echo "No .venv found. See README.md for install steps." >&2
    exit 1
fi

step() {
    echo
    echo "################################################################"
    echo "#  $1"
    echo "################################################################"
    if [ "${PAUSE:-0}" = "1" ]; then
        read -r -p "Press Enter to continue..."
    fi
}

step "1. Generate the synthetic trip event stream"
"$PY" generator.py

step "2. Start clean: delete lake/"
rm -rf lake
echo "lake/ deleted"

step "3. Stream days 1-3 (event-time windows, watermark, side output)"
"$PY" consumer.py --through-day 3 | last_batch

step "4. Schema evolution: surge_multiplier arrives on day 4"
"$PY" demo_schema.py

step "5. Stream days 5-6"
"$PY" consumer.py --through-day 6 | last_batch

step "6. Drain the next-morning arrivals into the side output"
"$PY" consumer.py --late-examples 20

step "7. Health signals: event vs processing time, side-output rate, completeness"
"$PY" demo_queries.py

step "8. Incident replay: the bad late-data correction"
"$PY" bad_correction.py

step "9. Time travel: prove the prior state, find the bad commit, restore"
"$PY" demo_time_travel.py

echo
echo "Demo complete."
